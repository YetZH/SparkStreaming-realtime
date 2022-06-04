package com.gamll.publishrealtime.dao.impl;

import com.gamll.publishrealtime.bean.NameValue;
import com.gamll.publishrealtime.dao.PublishDao;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
@Slf4j
public class PublishDaoImpl implements PublishDao {
    @Autowired
    RestHighLevelClient esClient;

    private String dauindexNmaePrefix = "gmall_day_info_";
    private String orderindexNmaePrefix = "gmall_order_wide_";


    /**
     * @param itemName
     * @param date
     * @param field    age => user_age  gender=>user_gender
     * @return
     */
//     todo  http://bigdata.gmall.com/statsByItem?itemName=小米手机&date=2021-02-02&t=gender
    @Override
    public List<NameValue> searchStatsByItem(String itemName, String date, String field) {
        ArrayList<NameValue> results = new ArrayList<>();
        String indexname = orderindexNmaePrefix + date;
        SearchRequest searchRequest = new SearchRequest(indexname);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//  不要明细
        sourceBuilder.size(0);
//        query
        MatchQueryBuilder sku_names = QueryBuilders.matchQuery("sku_name", itemName);
        sku_names.operator(Operator.AND);

        sourceBuilder.query(sku_names);

//        group
//        按field分组 求count
//        AggregatorFactories.Builder aggregations = sourceBuilder.aggregations();
//        log.info(field);
//                TermsAggregationBuilder groupbyAgg = AggregationBuilders.terms("groupby_" + field).field(field).size(100);
//
//        aggregations.addAggregator(groupbyAgg);
////       再求sum
//        aggregations.addAggregator(AggregationBuilders.sum("total_amount").field("split_total_amount"));

        TermsAggregationBuilder groupbyAgg = AggregationBuilders.terms("groupby_" + field).field(field).size(100);
        SumAggregationBuilder sumAgg = AggregationBuilders.sum("total_amount").field("split_total_amount");
        groupbyAgg.subAggregation(sumAgg);
        sourceBuilder.aggregation(groupbyAgg);

        searchRequest.source(sourceBuilder);

        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregationsResult = searchResponse.getAggregations();
            ParsedTerms parsedTerms = aggregationsResult.get("groupby_" + field);
            for (Terms.Bucket bucket : parsedTerms.getBuckets()) {
                String keyAsString = bucket.getKeyAsString();
                Aggregations bucketAggregations = bucket.getAggregations();
                ParsedSum parsedsum = bucketAggregations.get("total_amount");
                double value = parsedsum.getValue();
                    results.add(new NameValue(keyAsString,value));
            }
        } catch (ElasticsearchStatusException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                log.warn(indexname + "不存在......");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES失败......");
        }

        return results;
    }

    @Override
    public Map<String, Object> searchDau(String td) {
//        从ES中查询结果
        Map<String, Object> dauResults = new HashMap<>();

        Long dauTotal = searchDauTotal(td);
        dauResults.put("dauTotal", dauTotal);
//                今日今时明细
        Map<String, Object> dauTd = searchdauTd(td);
        dauResults.put("dauTd", dauTd);
//                昨日今时明细
        LocalDate nowdate = LocalDate.parse(td);
        LocalDate lastDay = nowdate.minusDays(1);
        Map<String, Object> dauYd = searchdauTd(lastDay.toString());

        dauResults.put("dauYd", dauYd);
        return dauResults;
    }

    private Map<String, Object> searchdauTd(String td) {
        Map<String, Object> re = new HashMap<>();
        String name = dauindexNmaePrefix + td;
        SearchRequest searchRequest = new SearchRequest(name);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);

        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("groupbyhr").field("hr.keyword").size(24);
        sourceBuilder.aggregation(termsAggregationBuilder);
        searchRequest.source(sourceBuilder);

        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            Aggregations aggregations = searchResponse.getAggregations();
            ParsedTerms parsedTerms = aggregations.get("groupbyhr");
            List<? extends Terms.Bucket> buckets = parsedTerms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                String hr = bucket.getKeyAsString();
                long hrTotal = bucket.getDocCount();
                re.put(hr, hrTotal);
            }
            return re;
        } catch (ElasticsearchStatusException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                log.warn(name + "不存在......");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES失败......");
        }
        return re;

    }

    public Long searchDauTotal(String td) {
        String name = dauindexNmaePrefix + td;
        SearchRequest searchRequest = new SearchRequest(name);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        不要明细
        sourceBuilder.size(0);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse searchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT);
            long dauTotal = searchResponse.getHits().getTotalHits().value;
            return dauTotal;
        } catch (ElasticsearchStatusException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                log.warn(name + "不存在......");
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("查询ES失败......");
        }
        return 0L;
    }
}
