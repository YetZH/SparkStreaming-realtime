package com.gmall.realtime.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import java.util
import scala.collection.mutable

/**
 * Offset管理工具类，用于往offset中存储和读取offset
 * 管理方案：
 *  1. 需要手动提交offset 且先处理 后提交
 *     2. 手动控制偏移量提交 -> sparkStreaming提交了手动提交方案， 但是我们不能用 因为我们会对Dstream的结构进行转换
 *     3.  我们需要手动提取偏移量 维护到redis当中
 */
object MyOffsetUtils {
  //todo  1.  存 offset

  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    if (offsetRanges != null && offsetRanges.length > 0) {
      val offsets = new util.HashMap[String, String]()
      for (offsetRange <- offsetRanges) {
        val partition: Int = offsetRange.partition
        val untilOffset: Long = offsetRange.untilOffset
        offsets.put(partition.toString, untilOffset.toString)
      }
      println("提交了offset："+offsets)
      //往redis中存
      val jedis: Jedis = MyRedisUtils.getJedisFromPool()
      val redisKey: String = s"offsets$topic:$groupId"
      jedis.hset(redisKey, offsets)

      jedis.close()
    }
  }

  /**
   * 从redis中读取存储的offset
   *
   * 问题：
   *      1.如何让sparkStringaming通过指定的offset进行消费
   *
   *      2.sparkStreaming要求的offset格式要求是什么?
   *            Map[TopicPartition,Long]   //主题分区，offset
  */
  def readOffset(topic : String,groupId:String): Map[TopicPartition,Long]  ={
    val jedis: Jedis = MyRedisUtils.getJedisFromPool()
    val redisKey: String = s"offsets$topic:$groupId"
    val offsets: util.Map[String, String] = jedis.hgetAll(redisKey)
    println("读取到offset：" +offsets)
    val results: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()
    //将java的map转换为scala的map进行迭代
    import scala.collection.JavaConverters._
    for ((partition,offset) <- offsets.asScala) {
      val tp = new TopicPartition(topic, partition.toInt)
      results.put(tp,offset.toLong)
//      put也可以
    }

    jedis.close()
    results.toMap
  }


}
