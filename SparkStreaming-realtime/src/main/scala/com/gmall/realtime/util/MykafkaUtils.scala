package com.gmall.realtime.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util
import scala.collection.mutable

object MykafkaUtils {
  /**
   * 消费者配置信息
   *
   * ConsumerConfig
   */
  private val consumerConfigs: mutable.Map[String, Object] = mutable.Map[String, Object](
    //kafka集群位置
//    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropertiesUtils.apply(Myconfig.KAFKA_BOOTSTRAP_SERVERS),

    // kv反序列化器
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    //group ID
    //  todo 传过来
    //offset提交
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    // 自动提交时间间隔 默认就是5s
//    ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "5000",
    //offset重置   “latest” "earliest"
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
    //...
  )

  /**
   * 基于SparkStreaming消费，获取到KafkaDstream ,使用默认Offset
   */
  def getKafkaDstream(ssc: StreamingContext, topic: String, groupid: String) = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupid)

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs)
    )
    kafkaDStream
  }

  /**
   *   基于SparkStreaming消费 使用指定Offset，获取到KafkaDstream
   *  @return
   */
  def getKafkaDstream(ssc: StreamingContext, topic: String, groupid: String,offsets:Map[TopicPartition, Long] )
  = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupid)

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs,offsets)
    )
    kafkaDStream
  }


  /**
   * 生产者对象
   */
  val producer: KafkaProducer[String, String] = createProducer()

  /**
   * 创建生产者对象
   */
  def createProducer() = {
    val producerConfigs = new util.HashMap[String, AnyRef]()
    //kafka集群位置
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  MyPropertiesUtils(Myconfig.KAFKA_BOOTSTRAP_SERVERS))
    //    KV序列化器
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    //acks
//    producerConfigs.put(ProducerConfig.ACKS_CONFIG, "0")
    //              默认
    // batch.size  16kb
    //linger.ms     0ms
    //retries   0

    //幂等性   默认false
    producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](producerConfigs)
    producer
  }

  /**
   * 生产（按照默认粘性分区策略） 因为没有指定offset与key
   *
   * @param topic
   * @param msg
   */
  def send(topic: String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, msg))
  }

  /**
   * 生产（按照key的hash进行分区） (key & Integer.MAX_VALUE) %分区个数
   *
   * @param topic
   * @param msg
   */
  def send(topic: String, key: String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, key, msg))
  }

  /**
   * 关闭生产者对象
   */
  def close: Unit = {
    if (producer != null) producer.close()
  }

  /**
   * 刷写生产者缓冲区
   */
  def flush(): Unit ={
    producer.flush()
  }
}
