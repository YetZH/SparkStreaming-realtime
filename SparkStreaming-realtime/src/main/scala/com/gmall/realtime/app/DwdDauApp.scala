package com.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.gmall.realtime.bean.{DauInfo, PageLog}
import com.gmall.realtime.util.{MyBeanUtils, MyEsUtils, MyOffsetUtils, MyRedisUtils, MykafkaUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
import java.util.Date
import scala.collection.mutable.ListBuffer


/** 日活宽表
 *
 * 1. 准备实时环境
 * 2. 从Redis中读取偏移量
 * 3. 从kafka中消费数据
 * 4. 提取偏移量结束点
 * 5. 处理数据
 * 5.1 转移数据结构
 * 5.2 去重
 * 5.3 维度关联
 * 6. 写入ES
 * 7. 提交offets
 *
 */
//todo  求日活
object DwdDauApp {
  def main(args: Array[String]): Unit = {

    //    1. 准备实时环境
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("DwdDayApp")
    val ssc = new StreamingContext(conf, Seconds(2))
    //    从redis中读取偏移量
    val topicName = "DWD_PAGE_LOG_TOPIC"
    val groupId = "DWD_DAU_GROUP"
    val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(topicName, groupId)
    //    3. 从kafka中消费数据
    var kafkaDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      kafkaDstream = MykafkaUtils.getKafkaDstream(ssc, topicName, groupId, offsets)
    } else {
      kafkaDstream = MykafkaUtils.getKafkaDstream(ssc, topicName, groupId)
    }
    //    4. 提取偏移量结束点
    var offsetRanges: Array[OffsetRange] = null
    val OffsetRangesDstream: DStream[ConsumerRecord[String, String]] = kafkaDstream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    //    5. 处理数据
    //     5.1 转换数据结构
    val PageLogDstream: DStream[PageLog] = OffsetRangesDstream.map(
      ConsumerRecord => {
        val value: String = ConsumerRecord.value()
        val pageLog: PageLog = JSON.parseObject(value, classOf[PageLog])
        pageLog
      }
    )
    //    自我审查
    //    将页面访问数据中 上页id （last_page_id） 不为空的过滤掉
    val filterPageLogDstream: DStream[PageLog] = PageLogDstream.filter(pageloge => {
      pageloge.last_page_id == null
    })
    //    第三方审查： 通过redis将当日活跃的mid维护起来，自我审查后过滤出来的每条数据都要到redis中进行比对去重
    //        redis 如何维护日活状态?
    //    类型：  list/set
    //    key:   DAY:DATE
    //    value:    mid的集合
    //    写入VPI:  Lpush/rpush       sadd
    //    写出API:          smembers/sismember
    //    过期：  24h
    //todo 用filter 每条数据执行一次 太频繁

    //    filterPageLogDstream.filter
    val redisFilterDstream: DStream[PageLog] = filterPageLogDstream.mapPartitions(IterPageLog => {
      val pageloges: ListBuffer[PageLog] = ListBuffer[PageLog]()
      val Dateformat = new SimpleDateFormat("yyyy-MM-dd")
      val jedis = MyRedisUtils.getJedisFromPool() // 一个批次开启一次
      for (pagelog <- IterPageLog) {
        val mid: String = pagelog.mid
        val date = new Date(pagelog.ts)
        val dateStr: String = Dateformat.format(date)
        val redisKey = s"DAU:$dateStr"

        if (!jedis.sismember(redisKey, mid)) {
          jedis.sadd(redisKey, mid)
          pageloges.append(pagelog);
        }
      }
      jedis.close()
      pageloges.iterator
    })
    //    5.3 维度关联
    val DauInfoDstream: DStream[DauInfo] = redisFilterDstream.mapPartitions(
      pageLogIter => {
        val DauInfoes: ListBuffer[DauInfo] = ListBuffer[DauInfo]()
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        for (pageLog <- pageLogIter) {
          val dauInfo = new DauInfo()
          //      笨办法： 一个一个挨个提取 然后赋值
          //          todo 好办法 使用java反射  写个工具类
          MyBeanUtils.copyProperties(pageLog, dauInfo)

          //          2.补充维度
          //           2.1用户维度
          val uid: String = pageLog.user_id
          val redisUidKey = s"DIM:USER_INFO:$uid"
          val userInfo: String = jedis.get(redisUidKey)
          val userInfoObj: JSONObject = JSON.parseObject(userInfo)

          //          提取性别
          val gender: String = userInfoObj.getString("gender")
          //          兴趣生日
          val birthday: String = userInfoObj.getString("birthday")
          //          换算年龄
          val birdate: LocalDate = LocalDate.parse(birthday)
          val nowDate: LocalDate = LocalDate.now()
          val age: Int = Period.between(birdate, nowDate).getYears
          //          补充到对象中
          dauInfo.user_gender = gender
          dauInfo.user_age = age.toString
          //          2.2地区信息维度
          val provinceID: String = dauInfo.province_id
          val redisProvinceKey = s"DIM:BASE_PROVINCE:$provinceID"
          val provinceJson: String = jedis.get(redisProvinceKey)
          val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)
          val provinceName: String = provinceJsonObj.getString("name")
          val provinceIsoCode: String = provinceJsonObj.getString("iso_code")
          val province3166: String = provinceJsonObj.getString(("iso_3166_2"))
          val provinceAreaCode: String = provinceJsonObj.getString("area_code")
          dauInfo.province_name = provinceName
          dauInfo.province_iso_code = provinceIsoCode
          dauInfo.province_3166_2 = province3166
          dauInfo.province_area_code = provinceAreaCode
          //          2.3 日期字段处理
          val date = new Date(pageLog.ts)
          val dtHr: String = dateFormat.format(date)
          val dtHrArr: Array[String] = dtHr.split(" ")
          dauInfo.dt = dtHrArr(0)
          dauInfo.hr = dtHrArr(1).split(":")(0)
          DauInfoes.append(dauInfo)
        }
        jedis.close()
        DauInfoes.iterator
      }
    )
    //    DauInfoDstream.print(100)

    //  todo
    //   写入到OLAP当中  上面数据已经处理好了 下面就是分析步骤
    //    写入OLAP
    //    按照天分割索引，通过索引模板控制mapping，setting，aliases登
    //    准备ES工具类
    DauInfoDstream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          dauInfoIter => {
            val docs: List[(String, DauInfo)] = dauInfoIter.map(dayinfo => (dayinfo.mid, dayinfo)).toList //doc_id, data
            //     索引名
            //  如果真实环境 直接获取当前日期 写入即可
            //  而我们是模拟数据 会生成不同日期的数据
            if (docs.nonEmpty) {
              // 获取日期
              val head: (String, DauInfo) = docs.head
              val ts: Long = head._2.ts
              val sdf = new SimpleDateFormat("yyyy-MM-dd")
              val dateStr: String = sdf.format(new Date(ts))
              val indexName: String = s"gmall_day_info_$dateStr"

              //            写入到ES中
              MyEsUtils.bulkSave(indexName, docs)
            }
           }
        )
//        提交Offset
        MyOffsetUtils.saveOffset(topicName,groupId,offsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination();
  }
  
}
