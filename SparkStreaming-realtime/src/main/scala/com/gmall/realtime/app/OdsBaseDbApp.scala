package com.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.gmall.realtime.util.{MyOffsetUtils, MyRedisUtils, MykafkaUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.util

/**
 * 业务数据消费分流
 *  1. 准备实时环境
 *
 * 2. 从redis中读取偏移量
 *
 * 3. 从kafka中消费数据
 *
 * 4. 提取偏移量结束点
 *
 * 5.数据处理   ***
 * 5.1 转换数据结构
 * 5.2 分流
 * 实时数据  =》 Kafka
 * 维度数据  =》 Redis
 * 6. flush kafka的缓冲区
 *
 * 7.提交offset
 */
object OdsBaseDbApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("ods_base_dp_app").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(2))
    val topicName: String = "ODS_BASE_DB"
    val groupId = "ODS_BASE_DB_GROUP"

    //3. 从kafka中消费数据
    val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(topicName, groupId)
    var kafkaDstream: InputDStream[ConsumerRecord[String, String]] = null;
    if (offsets != null && offsets.nonEmpty) {
      kafkaDstream = MykafkaUtils.getKafkaDstream(ssc, topicName, groupId, offsets)
    } else {
      kafkaDstream = MykafkaUtils.getKafkaDstream(ssc, topicName, groupId)
    }
    //4. 提取偏移量结束点
    var offsetRanges: Array[OffsetRange] = null
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
    //    jsonObjDstream.print(100)
    //    5.2 分流

    //    如何动态配置这个清单呢？
    //    将表清单维护到redis中，实时任务中动态的到redis中获取表清单.l/类型: set
    //     key: FACT:TABLES     DIM:TABLES
    //     value :表名的集合
    //    写入API: sadd
    //    读取API: smembers
    //    过期:  不过期


    //    事实表清单
//    val factTables: Array[String] = Array[String]("order_info", "order_detail" /*缺啥补啥*/)
    //    维度表清单
//    val dimTables: Array[String] = Array[String]("user_info", "base_province" /*缺啥补啥*/)

    jsonObjDstream.foreachRDD(rdd => {
      //      Driver   每个batch 读取一次
      val jedis: Jedis = MyRedisUtils.getJedisFromPool()

      val redisFactKey="FACT:TABLES"
      val redisDimKey="DIM:TABLES"
      //    事实表清单
      val factTables: util.Set[String] = jedis.smembers(redisFactKey)
      //    维度表清单
      val dimTables: util.Set[String] = jedis.smembers(redisDimKey)
      jedis.close()
//     todo  使用广播变量优化
      val factTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(factTables)
      val dimTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dimTables)
      rdd.foreachPartition(jsonObjIter => {
        //        Executor  todo 每个分区开启一个连接
        val jedis: Jedis = MyRedisUtils.getJedisFromPool()


        for (jsonObj <- jsonObjIter) {
          val OperType: String = jsonObj.getString("type")
          val opValue: String = OperType match {
            case "bootstrap-insert" => "I"
            case "insert" => "I"
            case "update" => "U"
            case "insert" => "D"
            case _ => null
          }
          //          判断操作类型 1. 明确是什么操作 2.过滤不感兴趣的数据
          if (opValue != null) {
            // 提取表名
            val tableName: String = jsonObj.getString("table")
            //            提取数据
            if (factTablesBC.value.contains(tableName)) {
              //              事实数据
              //              DWD_ORDER_INFO_I  DWD_ORDER_INFO_U  DWD_ORDER_INFO_d
              val data: String = jsonObj.getString("data")

              val dwdTopicName = s"DWD_${tableName.toUpperCase()}_$opValue"
              MykafkaUtils.send(dwdTopicName, data)
            } else if (dimTablesBC.value.contains(tableName)) {
              //              维度数据  往redis中存
              //              类型： String list set zset hash
              //              key:  DIM:表名:ID
              //              value:整条数据的jsonString
              //              写入API：set
              //              读取API： get
              //              过期：不过期
              val dataObj: JSONObject = jsonObj.getJSONObject("data")
              val id: String = dataObj.getString("id")
              val redisKey: String = s"DIM:${tableName.toUpperCase}:$id"
              //              在此处 开关 （new） jedis太频繁
              //      todo   Redis连接写到哪里???
              //               foreachRDD外面:driver ，连接对象不能序列化，不能传输
              //               foreachRDD里面，foreachPartition外面: driver，连接对象不能序列化，不能传输
              //               foreachPartition里面，循环外面:executor ,每分区数据开启一个连接，用完关闭.
              //               foreachPartition里面,循环里面: executor ，每条数据开启一个连接，用完关闭，太频繁。

              //              val jedis: Jedis = MyRedisUtils.getJedisFromPool()
              jedis.set(redisKey, dataObj.toString())

            }
          }
        }
        jedis.close()

        //        每分区的数据刷新一次
        MykafkaUtils.flush()
      })
      //      每batch提交一次offset
      MyOffsetUtils.saveOffset(topicName, groupId, offsetRanges)

    })

    ssc.start()
    ssc.awaitTermination()
  }
}
