package com.gmall.realtime.util

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType

/**
 * ES 工具类 用于对ES进行读写操作
 */
object MyEsUtils {


  //客户端对象
   val esClient = build()

  def build() = {
    val host: String = MyPropertiesUtils.apply(Myconfig.ES_HOST)
    val post: String = MyPropertiesUtils.apply(Myconfig.ES_PORT)

    val restClientBuilder: RestClientBuilder = RestClient.builder(new HttpHost(host, post.toInt))
    val client: RestHighLevelClient = new RestHighLevelClient(restClientBuilder)

//    val clients: RestHighLevelClient = new RestHighLevelClient(new RestClientBuilder( List(host,post.toInt)))
    client
  }

  /**
   * 关闭ES对象
   */
  def close(): Unit ={
    if(esClient !=null) esClient.close()
  }
  /**
   * 1. 批量写
   * 2. 幂等写
   */
  def bulkSave(indexName:String,docs: List[(String,AnyRef)]): Unit ={
    val bulkRequest: BulkRequest = new BulkRequest(indexName)
    for ((docID,docObj) <- docs) {
      val indexRequest = new IndexRequest()
      val dataJson: String = JSON.toJSONString(docObj, new SerializeConfig(true))
      indexRequest.source(dataJson,XContentType.JSON)
      indexRequest.id(docID)
      bulkRequest.add(indexRequest)
    }


   esClient.bulk(bulkRequest, RequestOptions.DEFAULT)



  }


}
