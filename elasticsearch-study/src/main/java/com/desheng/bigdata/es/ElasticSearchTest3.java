package com.desheng.bigdata.es;

import com.desheng.bigdata.es.util.TransportUtil;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 * 全文搜索
 *    中文分词问题
 */
public class ElasticSearchTest3 {

    private TransportClient client;
    @Before
    public void setUp() {
        client = TransportUtil.getClient();
    }

    final String[] indices =
    {"chinese1"};
    @Test
    public void testSearch() {
        SearchResponse response = client.prepareSearch(indices)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("content", "中国"))
                .get();
        printResponse(response);
    }

    public static void printResponse(SearchResponse response) {
        SearchHits searchHits = response.getHits();
        long totalHits = searchHits.totalHits;
        System.out.println("old李为您找到相关结果约" + totalHits + "个");
        float maxScore = searchHits.getMaxScore();
        System.out.println("最大得分：" + maxScore);
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            System.out.println("-------------&^_^&-------------");
            float score = hit.getScore();
            System.out.println("score: " + score);
            System.out.println(hit.getSourceAsString());
        }
    }


    @After
    public void cleanUp() {
        TransportUtil.release(client);
    }
}
