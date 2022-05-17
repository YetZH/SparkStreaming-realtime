package com.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.gmall.realtime.util.{MyOffsetUtils, MykafkaUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 业务数据消费分流
 *  1. 准备实时环境
 *
 *  2. 从redis中读取偏移量
 *
 *  3. 从kafka中消费数据
 *
 *  4. 提取偏移量结束点
 *
 *  5.数据处理   ***
 *      5.1 转换数据结构
 *      5.2 分流
 *            实时数据  =》 Kafka
 *            维度数据  =》 Redis
 *  6. flush kafka的缓冲区
 *
 *  7.提交offset
 */
object OdsBaseDbApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ods_base_dp_app").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(2))
    val topicName: String = "ODS_BASE_DB"
    val groupId = "ODS_BASE_DB_GROUP"

    //3. 从kafka中消费数据
    val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(topicName, groupId)
    var kafkaDstream: InputDStream[ConsumerRecord[String, String]]  = null;
    if(offsets !=null && offsets.nonEmpty ){
       kafkaDstream = MykafkaUtils.getKafkaDstream(ssc, topicName, groupId, offsets)
    }else {
      kafkaDstream =   MykafkaUtils.getKafkaDstream(ssc,topicName,groupId)
    }
    //4. 提取偏移量结束点
    var  offsetRanges: Array[OffsetRange] = null
    val offsetRangesDstream: DStream[ConsumerRecord[String, String]] = kafkaDstream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    //5. 处理数据
//      5.1 转换数据结构
val jsonObjDstream: DStream[JSONObject] = offsetRangesDstream.map(consumerRecord => {
  val value: String = consumerRecord.value()
  val jSONObject: JSONObject = JSON.parseObject(value)
  jSONObject
})
    jsonObjDstream.print(100)
    //    5.2 分流
//    jsonObjDstream.foreachRDD(rdd=>{
//      rdd.foreachPartition(jsonObjIter=>{
//        for (jsonObj <- jsonObjIter) {
//
//        }
//      })
//    })

    ssc.start()
    ssc.awaitTermination()
  }
}
