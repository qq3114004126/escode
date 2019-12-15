package com.desheng.bigdata.es;

import com.desheng.bigdata.es.pojo.BigdataProduct;
import com.desheng.bigdata.es.util.TransportUtil;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * 全文搜索
 *      client.prepareSearch(索引库)
 * 执行搜索类型（DEFAULT默认方式为Query then Fetch）：
 *    在5.3以下的时候有4中：
 *       Query and fetch
 *            查询所有的分片(个数为N)，然后每个分片返回size条记录，所以最终返回的结果数：N * size
 *       Query then Fetch
 *          查询相关的分片，在返回结果之前，先要进行结果排序和等分排名，返回最相关的size条记录。
 *       DFS Query and fetch
 *          在Query and fetch基础之上做了DFS
 *       DFS Query then fetch
 *          在Query then fetch基础之上做了DFS
 *
 *       DFS distributed frequency scatter 分布式词频发散：
 *          从es的官方网站我们可以发现，初始化散发其实就是在进行真正的查询之前，先把各个分片的词频率和文档频率收集一下，然后进行词搜索的时候，各分片依据全局的词频率和文档频率进行搜索和排名。
 *    在5.3以上基本就剩下2种了：
 *       Query then Fetch
 *       DFS Query then fetch
 * 检索方式setQuery
 *     query体现的就是sql语句中where条件后面写的内容
 * 排序
 * 分页
 * 高亮
 */
public class ElasticSearchTest2 {

    private TransportClient client;
    @Before
    public void setUp() {
        client = TransportUtil.getClient();
    }

    final String[] indices = {"account"};

    @Test
    public void testSearch() {
        SearchResponse response = client.prepareSearch(indices)
                        .setSearchType(SearchType.QUERY_THEN_FETCH)
                        /*
                            matchAllQuery相当于扫描全表select * from t
                            curl -XGET http://bigdata01:9200/account/_search
                         */
//                        .setQuery(QueryBuilders.matchAllQuery())
                        /*
                            matchQuery:
                                name: 相当于字段
                                text: 要检索的值
                            整个相当于：select * from t where name like '%text%';
                         */
//                        .setQuery(QueryBuilders.matchQuery("address", "750 Hudson Avenue"))
                         /*
                             涉及到分词：
                                使用matchQuery可以查询导数据，而termQuery找不到数据，原因就在于是否需要对要检索的内容进行分词。
                                matchQuery是对检索的内容和索引库中的内容都进行分词
                                termQuery是对索引库中内容有分词，而检索的内容没有分词。
                          */
//                        .setQuery(QueryBuilders.termQuery("address", "750 Hudson Avenue"))
                        //以短语的方式进行检索，二者都不分词
                        .setQuery(QueryBuilders.matchPhraseQuery("address", "750 Hudson Avenue"))
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
            Map<String, Object> source = hit.getSourceAsMap();
            Object number = source.get("account_number");
            Object balance = source.get("balance");
            Object gender = source.get("gender");
            Object lastname = source.get("lastname");
            Object age = source.get("age");
            Object address = source.get("address");
            System.out.println("score:\t" + score);
            System.out.println("number:\t" + number);
            System.out.println("lastname:\t" + lastname);
            System.out.println("gender:\t" + gender);
            System.out.println("age:\t" + age);
            System.out.println("balance:\t" + balance);
            System.out.println("address:\t" + address);
        }
    }

    //全文检索之二次排序
    @Test
    public void testSearchSort() {
        SearchResponse response = client.prepareSearch(indices)
                    .setSearchType(SearchType.DEFAULT)
                    //在搜索引擎中，自动忽略大小写
                    .setQuery(QueryBuilders.matchQuery("address", "avenue"))
                    //排序
                    .addSort("age", SortOrder.DESC)
                    .addSort("balance", SortOrder.ASC)
                    .get();
        printResponse(response);
    }
    /*
        大结果集分页
            pageSize:
            from:
         查询第N页的内容：
             from=(N-1)*pageSize
     */
    @Test
    public void testSearchPage() {
        SearchResponse response = client.prepareSearch(indices)
                          .setSearchType(SearchType.DEFAULT)
                          //在搜索引擎中，自动忽略大小写
                          .setQuery(QueryBuilders.matchQuery("address", "avenue"))
                          .addSort("age", SortOrder.DESC)
                          .addSort("balance", SortOrder.ASC)
                          //分页
                          .setFrom(1)
                          .setSize(5)
                          .get();
        printResponse(response);
    }

    //高亮
    @Test
    public void testSearchHighlight() {
        SearchResponse response = client.prepareSearch(indices)
                      .setSearchType(SearchType.DEFAULT)
                      //在搜索引擎中，自动忽略大小写
                      .setQuery(QueryBuilders.matchQuery("address", "avenue"))
                      .addSort("age", SortOrder.DESC)
                      .addSort("balance", SortOrder.ASC)
                      //分页
                      .setFrom(1)
                      .setSize(5)
                      //高亮
                      .highlighter(
                              SearchSourceBuilder.highlight()
                              .preTags("<font style=\"color: red; font-size: 100px\">")//前置标签
                              .field("address")//高亮字段
                              .postTags("</font>")//后置标签
                      )
                      .get();
        printHighlightResponse(response);
    }

    private void printHighlightResponse(SearchResponse response) {
        SearchHits searchHits = response.getHits();
        long totalHits = searchHits.totalHits;
        System.out.println("old李为您找到相关结果约" + totalHits + "个");
        float maxScore = searchHits.getMaxScore();
        System.out.println("最大得分：" + maxScore);
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            System.out.println("-------------&^_^&-------------");
            float score = hit.getScore();
            Map<String, Object> source = hit.getSourceAsMap();
            Object number = source.get("account_number");
            Object balance = source.get("balance");
            Object gender = source.get("gender");
            Object lastname = source.get("lastname");
            Object age = source.get("age");
            String address = "";
//            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
//            for (Map.Entry<String, HighlightField> me : highlightFields.entrySet()) {
//                String key = me.getKey();//高亮显示对应的字段
//                HighlightField highlightField = me.getValue();
//                Text[] texts = highlightField.fragments();//高亮显示的片段内容
//                for(Text text : texts) {
//                    address += text.toString();
//                }
//            }
            for(Text text : hit.getHighlightFields().get("address").fragments()) {
                address += text.toString();
            }
            System.out.println("score:\t" + score);
            System.out.println("number:\t" + number);
            System.out.println("lastname:\t" + lastname);
            System.out.println("gender:\t" + gender);
            System.out.println("age:\t" + age);
            System.out.println("balance:\t" + balance);
            System.out.println("address:\t" + address);
        }
    }

    //elasticsearch的聚合操作
    @Test
    public void testSearchAggr() {
        SearchResponse response = client.prepareSearch(indices)
                    .setSearchType(SearchType.DEFAULT)
                    .setQuery(QueryBuilders.matchQuery("address", "avenue"))
                    //select avg(balance) avg_balance from t where address like '%avenue%';
                    .addAggregation(
                            AggregationBuilders.avg("avg_balance")
                            .field("balance")
                    )
                    .get();
        Aggregations aggregations = response.getAggregations();
        for(Aggregation aggregation : aggregations) {
            Avg avg = (Avg)aggregation;
            String name = avg.getName();//得到聚合名称
            String type = avg.getType();//聚合类型
            System.out.println("name: " + name);
            System.out.println("type: " + type);
            System.out.println("value: " + avg.getValue());
        }
    }

    @After
    public void cleanUp() {
        TransportUtil.release(client);
    }
}
