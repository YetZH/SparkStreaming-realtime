package com.gmall.realtime.util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object MyRedisUtils {
//  先通过连接池对象
  var jedisPool : JedisPool = null

  def  getJedisFromPool() ={
    if(jedisPool == null){
      //创建连接池对象
//      连接池配置
      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100)  //最大连接数
      jedisPoolConfig.setMaxIdle(20)    //最大空闲数
      jedisPoolConfig.setMinIdle(20)    //最小空闲数
      jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(5000)  //忙碌时等待时长 ms
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

      val host: String = MyPropertiesUtils(Myconfig.REDIS_HOST)
      val port: String = MyPropertiesUtils(Myconfig.REDIS_PORT)
      //创建连接池对象
      jedisPool = new JedisPool(jedisPoolConfig,host,port.toInt)
    }
    jedisPool.getResource
  }

}
