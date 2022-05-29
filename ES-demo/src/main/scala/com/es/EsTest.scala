package com.es

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.{GetRequest, GetResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.text.Text
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilders, TermQueryBuilder}
import org.elasticsearch.index.reindex.UpdateByQueryRequest
import org.elasticsearch.script.{Script, ScriptType}
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.aggregations.bucket.terms.{ParsedTerms, Terms, TermsAggregationBuilder}
import org.elasticsearch.search.aggregations.metrics.{AvgAggregationBuilder, ParsedAvg}
import org.elasticsearch.search.aggregations.{AggregationBuilders, Aggregations, BucketOrder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.{HighlightBuilder, HighlightField}
import org.elasticsearch.search.sort.SortOrder

import java.util

/**
 * ES客户端
 */
object EsTest {
  def main(args: Array[String]): Unit = {
    searchByAggs()
    close()
  }

  var client: RestHighLevelClient = create()

  def create() = {
    val restClientBuilder: RestClientBuilder = RestClient.builder(new HttpHost("hadoop102", 9200))
    val client: RestHighLevelClient = new RestHighLevelClient(restClientBuilder)
    client
  }

  def close() = {
    if (client != null) {
      client.close()
    }
  }

  /**
   * 查询 -  单条查询
   */
  def getById(): Unit = {

    val request: GetRequest = new GetRequest("movie_test", "1002")
    val getResponse: GetResponse = client.get(request, RequestOptions.DEFAULT)
    val dataStr: String = getResponse.getSourceAsString
    println(dataStr)
  }

  /**
   * 查询 - 条件查询
   */

  /**
   * search :
   * 查询 doubanScore>=5.0 关键词搜索 red sea
   * 关键词高亮显示
   * 显示第一页，每页 2 条
   * 按
   * */
  def searchByFilter(): Unit = {
    val searchRequest: SearchRequest = new SearchRequest("movie_index")
    val searchSourceBuilder = new SearchSourceBuilder()
    //    query
    //    bool
    val boolQueryBuilder: BoolQueryBuilder = QueryBuilders.boolQuery()
    //    filter
    boolQueryBuilder.filter(QueryBuilders.rangeQuery("doubanScore").gte(5.0))
    //    must
    boolQueryBuilder.must(QueryBuilders.matchQuery("name", "red sea"))

    searchSourceBuilder.query(boolQueryBuilder)
    //    分页
    searchSourceBuilder.from(0)
    searchSourceBuilder.size(2)
    //    排序
    searchSourceBuilder.sort("doubanScore", SortOrder.DESC)

    //    高亮
    searchSourceBuilder.highlighter(new HighlightBuilder().field("name"))

    searchRequest.source(searchSourceBuilder)
    val searchResponse: SearchResponse = client.search(searchRequest, RequestOptions.DEFAULT)
    //    获取总条数据
    val totalDocs: Long = searchResponse.getHits.getTotalHits.value

    //    获取明细数据
    val hits: Array[SearchHit] = searchResponse.getHits.getHits
    for (hit <- hits) {
      //数据
      val dataJson: String = hit.getSourceAsString
      //      提取高亮
      val fields: util.Map[String, HighlightField] = hit.getHighlightFields
      val field: HighlightField = fields.get("name")
      val fragments: Array[Text] = field.getFragments
      val highString: String = fragments(0).toString

      println("明细数据" + dataJson)
      println("高亮" + highString)
    }
  }

  /**
   * 查询 - 聚合查询
   * 查询每位演员参演的电影的平均分，倒序排序
   */
  def searchByAggs(): Unit = {

    val searchRequest = new SearchRequest("movie_index")

    val builder: SearchSourceBuilder = new SearchSourceBuilder()
    //不要明细
    builder.size(0)
    //    group
    val termsAggregationBuilder: TermsAggregationBuilder = AggregationBuilders.terms("groupbyactorname")
      .field("actorList.name.keyword").size(10).order(BucketOrder.aggregation("doubanScoreAvg", false))
    //    求avg
    val avgAggregationBuilder: AvgAggregationBuilder = AggregationBuilders.avg("doubanScoreAvg").field("doubanScore")

    termsAggregationBuilder.subAggregation(avgAggregationBuilder)
      builder.aggregation(termsAggregationBuilder)
    searchRequest.source(builder)

    val searchResponse: SearchResponse = client.search(searchRequest, RequestOptions.DEFAULT)
    val aggregations: Aggregations = searchResponse.getAggregations
    val groupbyActorName: ParsedTerms = aggregations.get[ParsedTerms]("groupbyactorname")

    val buckets: util.List[_ <: Terms.Bucket] = groupbyActorName.getBuckets
    import scala.collection.JavaConverters._
    for (bucket <- buckets.asScala) {
      //      名字
      val actorNumber: String = bucket.getKeyAsString
      //      电影个数
      val count: Long = bucket.getDocCount
      //      平均分
      val aggregations: Aggregations = bucket.getAggregations
      val doubanScoreAvgParsedAvg: ParsedAvg = aggregations.get[ParsedAvg]("doubanScoreAvg")
      val avgScore: Double = doubanScoreAvgParsedAvg.getValue

      println(s"$actorNumber 共参演了 $count 部电影，平均分为$avgScore")
    }
  }

  /**
   * 删除
   */

  def delete(): Unit = {
    val deleteRequest: DeleteRequest = new DeleteRequest("movie_test", "mWL9BYEBB35ltObRAIUg")
    client.delete(deleteRequest, RequestOptions.DEFAULT)
  }

  //todo 修改用的很少

  /**
   * 修改  - 单挑修改
   */
  def update(): Unit = {
    val updateRequest: UpdateRequest = new UpdateRequest("movie_test", "1002")
    updateRequest.doc("movie_name", "飞机大战1111")
    client.update(updateRequest, RequestOptions.DEFAULT)
  }

  /**
   * 修改 -  条件修改
   */
  def updateByQuery(): Unit = {
    //query
    val updateByQueryRequest: UpdateByQueryRequest = new UpdateByQueryRequest("movie_test")
    //  new TermQueryBuilder("movie_name.keyword","111")
    val boolQueryBuilder: BoolQueryBuilder = QueryBuilders.boolQuery()
    val termQueryBuilder: TermQueryBuilder = QueryBuilders.termQuery("movie_name.keyword", "速度与激情3")
    boolQueryBuilder.filter(termQueryBuilder)

    updateByQueryRequest.setQuery(boolQueryBuilder)
    //update
    val parmas = new util.HashMap[String, AnyRef]
    parmas.put("newName", "阿三大苏打111MYF")
    val script: Script = new Script(ScriptType.INLINE, "painless", "ctx._source['movie_name']=params.newName", parmas)
    updateByQueryRequest.setScript(script)

    client.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT)
  }

  /**
   * 批量写
   */
  def bulk(): Unit = {
    val bulkRequest = new BulkRequest()
    val movies: List[Movie] = List[Movie](
      Movie("1002", "长津湖"),
      Movie("1003", "喜羊羊"),
      Movie("1004", "狙击手"),
      Movie("1005", "熊出没")
    )
    for (movie <- movies) {
      //      指定索引
      val movierequest: IndexRequest = new IndexRequest("movie_test")
      val movieJSON: String = JSON.toJSONString(movie, new SerializeConfig(true))
      movierequest.source(movieJSON, XContentType.JSON)
      //      幂等性 就指定id 非幂等就不指定id
      movierequest.id(movie.id)
      //      将movierequest添加入bulk
      bulkRequest.add(movierequest)
    }
    client.bulk(bulkRequest, RequestOptions.DEFAULT)
  }

  /**
   * 增 - 幂等
   */
  def put(): Unit = {
    val indexRequest: IndexRequest = new IndexRequest()
    //    指定索引
    indexRequest.index("movie_index")
    //    指定_doc
    val movie: Movie = Movie("1001", "速度与激情4")
    val movieJson = JSON.toJSONString(movie, new SerializeConfig(true))
    indexRequest.source(movieJson, XContentType.JSON)
    //    指定doc id
    indexRequest.id("1001")
    client.index(indexRequest, RequestOptions.DEFAULT)
  }

  /**
   * 增 - 非幂等
   */
  def post(): Unit = {
    val indexRequest: IndexRequest = new IndexRequest()
    //    指定索引
    indexRequest.index("movie_test")
    //    指定_doc
    val movie: Movie = Movie("1001", "速度与激情3")
    val movieJson = JSON.toJSONString(movie, new SerializeConfig(true))
    indexRequest.source(movieJson, XContentType.JSON)
    //       todo 不指定doc id 就是非幂等写
    //    indexRequest.id("1001")
    client.index(indexRequest, RequestOptions.DEFAULT)
  }
}

case class Movie(id: String, movie_name: String)