package com.gmall.realtime.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.gmall.realtime.bean.{PageActionLog, PageDisplayLog, PageLog, StartLog}
import com.gmall.realtime.util.{MyOffsetUtils, MykafkaUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConverters._

/**
 * 日志数据消费分流
 * 1.准备实时处理环境 streamingContext
 * 2.从kafka消费数据
 * 3.处理数据
 * 3.1 转化数据结构
 * 专用结构  （Bean）
 * 通用结构   Map  JsonObject
 * 3.2 分流操作
 * 4. 写出到DWD层（再写回DWD层）
 */
object OdsBaseLogApp {
  def main(args: Array[String]): Unit = {
    //1.准备实时环境
    // todo 注意并行度与kafka中topic的分区个数的对应关系
    val conf = new SparkConf().setAppName("ods_base_log_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    val DWD_PAGE_LOG_TOPIC = "DWD_PAGE_LOG_TOPIC" //页面访问
    val DWD_PAGE_DISPLAY_TOPIC = "DWD_PAGE_DISPLAY_TOPIC" //页面曝光
    val DWD_PAGE_ACTION_TOPIC = "DWD_PAGE_ACTION_TOPIC" // 页面动作日志（事件）
    val DWD_START_LOG_TOPIC = "DWD_START_LOG_TOPIC" //启动日志（数据）
    val DWD_ERROR_LOG_TOPIC = "DWD_ERROR_LOG_TOPIC" //错误日志（数据）
    //2. 从kafka中获取数据
    val topicName = "ODS_BASE_LOG" //对应生成器配置中的主题名
    val groupID = "ODS_BASE_LOG_GROUP"
    //todo 补充  从Redis中读取offset，指定offset进行消费
    val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(topicName, groupID)
    var kafkaDstream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      kafkaDstream = MykafkaUtils.getKafkaDstream(ssc, topicName, groupID, offsets)
    } else {
      kafkaDstream = MykafkaUtils.getKafkaDstream(ssc, topicName, groupID)
    }
    //    val kafkaDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtils.getKafkaDstream(ssc, topicName, groupID)
    //    todo 补充： 从当前消费到的数据中提取offsets，当前操作不对流中数据做任何处理 只看一些东西
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDstream: DStream[ConsumerRecord[String, String]] = kafkaDstream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //在哪里执行? driver端执行
      rdd
    })


    //3. 处理数据
    //3.1 转换数据结构


    val jsonObjDstream: DStream[JSONObject] = offsetRangesDstream.map {
      ConsumerRecord => {
        //获取ConsumerRecord中的value  value就是日志数据
        val log: String = ConsumerRecord.value()
        val jsonObj: JSONObject = JSON.parseObject(log)
        jsonObj
      }
    }
    //        jsonObjDstream.print(100)
    //    3.2 分流操作
    //              页面访问数据
    //                  公共字段
    //                  页面数据
    //                  曝光数据
    //                  时间数据
    //                  错误数据
    //              启动数据
    //                  公共字段
    //                  启动数据
    //                  错误数据


    //todo 分流规则:
    //  错误日志：不做任何的拆分，只要包含错误字段，直接整条数据发给对应topic
    //  页面访问日志：拆分成页面访问，曝光，事件 分别发送到对应的topic
    //  启动日志: 直接发送到对应topic
    jsonObjDstream.foreachRDD(rdd => {
      rdd.foreach(jsonObj => {
        //分流操作
        //分流错误数据
        val errObbj: JSONObject = jsonObj.getJSONObject("err")
        if (errObbj != null) {
          //将错误数据发送到 DWD_ERROD_LOG_TOPIC
          MykafkaUtils.send(DWD_ERROR_LOG_TOPIC, jsonObj.toJSONString)
        } else {
          //页面日志
          val commonObj: JSONObject = jsonObj.getJSONObject("common")

          val ar: String = commonObj.getString("ar") //地区
          val uid: String = commonObj.getString("uid")
          val os: String = commonObj.getString("os")
          val ch: String = commonObj.getString("ch")
          val isNew: String = commonObj.getString("is_new")
          val md: String = commonObj.getString("md")
          val mid: String = commonObj.getString("mid")
          val vc: String = commonObj.getString("vc")
          val ba: String = commonObj.getString("ba") //品牌
          //提取时间戳
          val ts: Long = jsonObj.getLong("ts")
          //页面日志
          val pageObj: JSONObject = jsonObj.getJSONObject("page")
          if (pageObj != null) {
            //            如果不为空 就提取page字段
            val pageId: String = pageObj.getString("page_id")
            val pageItem: String = pageObj.getString("item")
            val pageItemType: String = pageObj.getString("item_type")
            val duringTime: Long = pageObj.getLong("during_time")
            val lastPageId: String = pageObj.getString("last_page_id")
            val sourceType: String = pageObj.getString("source_type")
            //        todo    发送到页面主题当中  DWD_PAGE_LOG_TOPIC
            val pageLog = PageLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId,
              pageItem, pageItemType, duringTime, sourceType, ts)

            MykafkaUtils.send(DWD_PAGE_LOG_TOPIC, JSON.toJSONString(pageLog, new SerializeConfig(true)))

            //            提取曝光数据
            val displaysJsonArr: JSONArray = jsonObj.getJSONArray("displays")

            if (displaysJsonArr != null && displaysJsonArr.size() > 0) {
              for (i <- 0 until (displaysJsonArr.size())) {
                //循环拿到每个曝光数据
                val displayObj: JSONObject = displaysJsonArr.getJSONObject(i)
                val displayType: String = displayObj.getString("display_type")
                val displayItem: String = displayObj.getString("item")
                val displayItemType: String = displayObj.getString("item_type")
                val posId: String = displayObj.getString("pos_id")
                val order: String = displayObj.getString("order")
                //                  封装成 bean对象
                val pageDisplayLog: PageDisplayLog = PageDisplayLog(mid, uid, ar, ch,
                  isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType,
                  duringTime, sourceType, displayType, displayItem, displayItemType, order, posId, ts)
                //                  写到 DWD_PAGE_DISPLAY_TOPIC
                MykafkaUtils.send(DWD_PAGE_DISPLAY_TOPIC, JSON.toJSONString(pageDisplayLog, new SerializeConfig(true)))
              }
            }
            //            提取动作日志
            val actionArray: JSONArray = jsonObj.getJSONArray("actions")
            if (actionArray != null && actionArray.size() > 0) {
              for (i <- 0 until actionArray.size()) {
                val actionObj = actionArray.getJSONObject(i)
                val actionItem: String = actionObj.getString("item")
                val actionId: String = actionObj.getString("action_id")
                val actionItemType: String = actionObj.getString("item_type")
                val actionTs: Long = actionObj.getLong("ts")

                val pageActionLog: PageActionLog = PageActionLog(mid, uid, ar, ch, isNew, md, os, vc,
                  ba, pageId, lastPageId, pageItem, pageItemType,
                  duringTime, sourceType, actionId, actionItem, actionItemType, actionTs, ts)
                //                写出到 DWD_PAGE_ACTION_TOPIC
                MykafkaUtils.send(DWD_PAGE_ACTION_TOPIC, JSON.toJSONString(pageActionLog, new SerializeConfig(true)))
              }
            }
          }
          val startObj: JSONObject = jsonObj.getJSONObject("start")
          if (startObj != null) {
            val entry: String = startObj.getString("entry")
            val openAndSkipMs: Long = startObj.getLong("open_ad_skip_ms")
            val openAndMs: Long = startObj.getLong("open_ad_ms")
            val loadingTime: Long = startObj.getLong("loading_time")
            val openAndId: String = startObj.getString("open_ad_id")

            //            封装bean
            val startLog: StartLog = StartLog(mid, uid, ar, ch, isNew, md, os, vc, ba, entry, openAndId, loadingTime,
              openAndMs, openAndSkipMs, ts)

            //            写出到 DWD_START_LOG_TOPIC
            MykafkaUtils.send(DWD_START_LOG_TOPIC, JSON.toJSONString(startLog, new SerializeConfig(true)))
          }
        }
        // foreach 里面 提交offset？  是在executor端执行 每条数据执行一次 太慢 不行
      }
      )
      // todo foreachRDD 里面提交offset？  Driver端执行， 一个batch（批次）执行一次
      MyOffsetUtils.saveOffset(topicName,groupID,offsetRanges)
    }
    )
    //foreachRDD 外面提交offset？ 是才Driver端 但是呢每次启动程序就执行一次
    ssc.start()
    ssc.awaitTermination()
  }

}
