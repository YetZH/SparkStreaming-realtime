package com.gmall.realtime.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.gmall.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.gmall.realtime.util.{MyEsUtils, MyOffsetUtils, MyRedisUtils, MykafkaUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.time.{LocalDate, Period}
import java.util
import scala.collection.mutable.ListBuffer

/**
 * 订单宽表任务
 *
 * 1.准备实时环境
 * 2.从Redis中读取Offset *2
 * 3.从kafka中消费数据 *2
 * 4.提取Offset  *2
 * 5.数据处理
 * 5.1 转换结构
 * 5.2 维度关联
 * 5.3 双流join
 * 6.写入ES
 * 7.提交Offset * 2
 */
object DwdOrderApp {
  def main(args: Array[String]): Unit = {
    //   1. 准备环境
    val conf: SparkConf = new SparkConf().setAppName("dwd_order_app").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //   2.读取offset  两个流

    // order_info
    val orderInfoTopicName = "DWD_ORDER_INFO_I"
    val orderInfoGroupId = "DWD_ORDER_INFO:GROUP"
    val orderInfoOffsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(orderInfoTopicName, orderInfoGroupId)
    //    order_info流

    var orderInfoKafkaDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderInfoOffsets != null && orderInfoOffsets.nonEmpty) {
      orderInfoKafkaDstream = MykafkaUtils.getKafkaDstream(ssc, orderInfoTopicName, orderInfoGroupId, orderInfoOffsets)
    } else {
      orderInfoKafkaDstream = MykafkaUtils.getKafkaDstream(ssc, orderInfoTopicName, orderInfoGroupId)
    }
    //order _detail
    val orderDetailTopicName = "DWD_ORDER_DETAIL_I"
    val orderDetailGroupId = "DWD_ORDER_DETAIL:GROUP"
    val orderDetailOffsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(orderDetailTopicName, orderDetailGroupId)

    //    order_detail流
    var orderDeatilKafkaDstream: InputDStream[ConsumerRecord[String, String]] = null

    if (orderDetailOffsets != null && orderDetailOffsets.nonEmpty) {
      orderDeatilKafkaDstream = MykafkaUtils.getKafkaDstream(ssc, orderDetailTopicName, orderDetailGroupId, orderDetailOffsets)

    } else orderDeatilKafkaDstream = MykafkaUtils.getKafkaDstream(ssc, orderDetailTopicName, orderDetailGroupId)

    //    4.提取offset
    var OrderInfoOffsetRanges: Array[OffsetRange] = null;
    val orderInfoOffsetDstream: DStream[ConsumerRecord[String, String]] = orderInfoKafkaDstream.transform(rdd => {
      OrderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    var orderDetailOffsetRanges: Array[OffsetRange] = null
    val orderDetailOffsetDstream: DStream[ConsumerRecord[String, String]] = orderDeatilKafkaDstream.transform(rdd => {
      orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    //        5. 处理数据
    //          5.1转换结构
    val orderInfoDstream: DStream[OrderInfo] = orderInfoOffsetDstream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])
        orderInfo
      }
    )
    //    orderInfoDstream.print(100)
    val orderDetailDstream: DStream[OrderDetail] = orderDetailOffsetDstream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        JSON.parseObject(value, classOf[OrderDetail])
      }
    )
    //    orderDetailDstream.print(100)
    //   todo 5.2 维度关联 只做Order_info
    val OrderInfoDimDstream: DStream[OrderInfo] = orderInfoDstream.mapPartitions(OrderInfoIter => {
      //      也可以直接将OrderInfoIter 转到list
      //      val orderInfoes: List[OrderInfo] = OrderInfoIter.toList
      val orderInfoes: ListBuffer[OrderInfo] = ListBuffer[OrderInfo]()
      val jedis: Jedis = MyRedisUtils.getJedisFromPool()

      for (orderInfo <- OrderInfoIter) {
        //        关联用户维度
        val uid: Long = orderInfo.user_id
        val redisUserKey: String = s"DIM:USER_INFO:$uid"
        val userInfoJson: String = jedis.get(redisUserKey)
        val userInfoJsonObj: JSONObject = JSON.parseObject(userInfoJson)
        //        提取性别
        val gender: String = userInfoJsonObj.getString("gender")
        //        提取生日
        val birthday: String = userInfoJsonObj.getString("birthday")
        //        换算年龄
        val nowDate: LocalDate = LocalDate.now()
        val birthdayLd: LocalDate = LocalDate.parse(birthday)
        //        nowDate.getYear  - birthdayLd.getYear  这样只能判断年 21.5 23.1  年龄等于2 其实是1
        val age: Int = Period.between(birthdayLd, nowDate).getYears
        //         补充到对象中
        orderInfo.user_gender = gender
        orderInfo.user_age = age
        //        关联地区维度 "DIM:BASE_PROVINCE:18"
        val provinceId: Long = orderInfo.province_id
        val redisProvinceKey: String = s"DIM:BASE_PROVINCE:$provinceId"

        val redisProvinceJson: String = jedis.get(redisProvinceKey)
        val ProvincejsonObj: JSONObject = JSON.parseObject(redisProvinceJson)

        val provinceAreacode: String = ProvincejsonObj.getString("area_code")
        val provinceName: String = ProvincejsonObj.getString("name")
        val pronvinceIso3166: String = ProvincejsonObj.getString("iso_3166_2")
        val IsoCode: String = ProvincejsonObj.getString("iso_code")
        orderInfo.province_name = provinceName
        orderInfo.province_area_code = provinceAreacode
        orderInfo.province_3166_2_code = pronvinceIso3166
        orderInfo.province_iso_code = IsoCode

        //        处理日期字段
        val operateTime: String = orderInfo.create_time
        //        val operateDate: LocalDate = LocalDate.parse(operateTime)
        //        operateDate.format(new DateTimeFormatter("yyyy-mm-dd"))
        val operateTimeIter: Array[String] = operateTime.split(" ")
        val create_date: String = operateTimeIter(0)
        val hour = operateTimeIter(1).split(":")(0)
        orderInfo.create_date = create_date
        orderInfo.create_hour = hour

        orderInfoes.append(orderInfo)
      }
      jedis.close()
      orderInfoes.iterator
    })

    //5.3 双流join
    //    内连接 join
    //   数据库层面： order info 中的数据 和order_detail表中的数据一定能关联成功
    val orderInfoKVDstream: DStream[(Long, OrderInfo)] = OrderInfoDimDstream.map(orderInfo => (orderInfo.id, orderInfo))

    val orderDetailKVDstream: DStream[(Long, OrderDetail)] = orderDetailDstream.map(orderDetail => (orderDetail.order_id, orderDetail))
    //       这样不行 可能数据join不上
    //    val joinResult: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoKVDstream.join(orderDetailKVDstream)

    //    解决
    //    首先使用FullOuterJoin,保证join成功或者没有成功都出现到结果中
    val orderJoinDstream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] =
    orderInfoKVDstream.fullOuterJoin(orderDetailKVDstream)

    val orderWideDstream: DStream[OrderWide] = orderJoinDstream.mapPartitions(
      orderJonIter => {
        val orderWides: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()
        for ((key, (orderInfoOp, orderDetailOp)) <- orderJonIter) {
          //          todo  关联后 一共三种情况
          //   orderInfoOp  有  orderDetailOp有
          if (orderInfoOp.isDefined) {
            //todo  也可以用orderInfoOp != None 不能!=null 因为这是Option对象里面只有Some和None* {
            //        取出OrderInfo
            val orderInfo: OrderInfo = orderInfoOp.get
            if (orderDetailOp.isDefined) {
              //              取出 OrderDetail
              val OrderDetail: OrderDetail = orderDetailOp.get
              //              组装
              orderWides.append(new OrderWide(orderInfo, OrderDetail))
            }
            //   orderInfoOp  有  orderDetailOp没有

            //OrderInfo读orderDetail的缓存
            val redisOrderDetailKey: String = s"ORDERJOIN:ORDER_Detail:${orderInfo.id}"
            val orderDetails: util.Set[String] = jedis.smembers(redisOrderDetailKey)
            if (orderDetails != null && orderDetails.size() > 0) {
              import scala.collection.JavaConverters._
              for (orderDetailJson <- orderDetails.asScala) {
                val OrderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
                orderWides.append(new OrderWide(orderInfo, OrderDetail))
              }
            }

            //   todo OrderInfo写缓存  不管有没有 都要写

            //              类型： String
            //              Key:    ORDERJOIN:ORDER_INFO:$ID
            //              value:  json
            //              写入API：set
            //              读取API：get
            //              是否过期： 24h??  我感觉几十S就行了
            val redisOrderInfoKey: String = s"ORDERJOIN:ORDER_INFO:${orderInfo.id}"
            //            jedis.set(redisOrderInfoKey,JSON.toJSONString(orderInfoOp.get,new SerializeConfig(true)))
            //            jedis.expire(redisOrderInfoKey,24*3600)
            jedis.setex(redisOrderInfoKey, 24 * 3600, JSON.toJSONString(orderInfoOp.get, new SerializeConfig(true)))

          } else {
            //   orderInfoOp  没有  orderDetailOp有
            val OrderDetail: OrderDetail = orderDetailOp.get
            //      先读缓存
            val redisOrderInfoKey: String = s"ORDERJOIN:ORDER_INFO:${OrderDetail.order_id}"
            val OrderInfoJson: String = jedis.get(redisOrderInfoKey)
            if (OrderInfoJson != null && OrderInfoJson.nonEmpty) {
              val orderInfo: OrderInfo = JSON.parseObject(OrderInfoJson, classOf[OrderInfo])
              val OrderDetail: OrderDetail = orderDetailOp.get
              //              加入到结果集中
              orderWides.append(new OrderWide(orderInfo, OrderDetail))
            } else {
              //            没读到就写orderDetail缓存
              //  写缓存
              //            类型：  set
              //            key：   ORDERJOIN:ORDER_Detail:$OrderID
              //            value：  json,json ....
              //            写入API： sadd
              //            读取api：smembers
              //            是否过期： 24h
              val redisOrderDetailKey: String = s"ORDERJOIN:ORDER_Detail:${OrderDetail.order_id}"
              jedis.sadd(redisOrderDetailKey, JSON.toJSONString(OrderDetail, new SerializeConfig(true)))
              jedis.expire(redisOrderDetailKey, 3600 * 24)
            }
          }
        }
        jedis.close()
        orderWides.iterator
      }
    )
    //    orderWideDstream.print(10000)
    //    写入OLAP
    //    按照天分割索引，通过索引模板控制mapping，setting，aliases登
    //    准备ES工具类
    orderWideDstream.foreachRDD(rdd => {
      rdd.foreachPartition(iterOrderWide => {
        val orderWides: List[(String, OrderWide)] = iterOrderWide.map(orderwide => (orderwide.detail_id.toString, orderwide)).toList
        if (orderWides.nonEmpty) {
          val head: (String, OrderWide) = orderWides.head
          val date: String = head._2.create_date
          val indexName: String = s"gmall_order_wide_$date"

          //            写入到ES中
          MyEsUtils.bulkSave(indexName, orderWides)
        }
      })
      // 提交offset 两份
      MyOffsetUtils.saveOffset(orderInfoTopicName,orderInfoGroupId,OrderInfoOffsetRanges)
      MyOffsetUtils.saveOffset(orderDetailTopicName,orderDetailGroupId,orderDetailOffsetRanges)
    })
    ssc.start()
    ssc.awaitTermination()

  }

}
