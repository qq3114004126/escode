package com.desheng.bigdata.es;

import com.desheng.bigdata.es.pojo.BigdataProduct;
import com.fasterxml.jackson.core.JsonParser;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
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
 * 学习elasticsearch的api操作
 *
 * 索引的操作：CRUD 批量
 *
 * 入口类：TransportClient
 *
 * elasticsearch集群默认的cluster.name为elasticsearch
 * 需要注意该异常：NoNodeAvailableException[None of the configured nodes are available 通常是没有指定cluster.name造成的
 */
public class ElasticSearchTest1 {

    private TransportClient client;
    @Before
    public void setUp() {
        Settings settings = Settings.builder()
                                    .put("cluster.name", "eshadoop")
                                    .build();
        client = new PreBuiltTransportClient(settings);
        TransportAddress trans1 = new TransportAddress(new InetSocketAddress("192.168.2.101", 9300));
        TransportAddress trans2 = new TransportAddress(new InetSocketAddress("192.168.2.102", 9300));
        TransportAddress trans3 = new TransportAddress(new InetSocketAddress("192.168.2.103", 9300));
        client.addTransportAddresses(trans1, trans2, trans3);
//        settings = client.settings();
//        for (String key : client.settings().keySet()) {
//            System.out.println(key + ":\t" + settings.get(key));
//        }
    }

    final String index = "product";
    final String type = "bigdata";
    @Test
    public void testGet() {
        GetResponse response = client.prepareGet(index, type, "1").get();//.execute().actionGet() = get()
        Map<String, Object> source = response.getSource();
        System.out.println(source);
    }
    /*
        json-str
        map
        java-bean
        XContentBuilder
     */
    @Test
    public void testCreateJSON() {
        String json ="{\"name\": \"hbase\", \"author\": \"oldli\", \"version\": 1.4}";
        IndexResponse response = client.prepareIndex(index, type, "10")
                                         .setSource(json, XContentType.JSON).get();
        System.out.println("version: " + response.getVersion());
    }

    @Test
    public void testCreateMap() {
        Map<String, Object> source = new HashMap<String, Object>();
        source.put("name", "sqoop");
        source.put("author", "cloudera");
        source.put("version", 1.4);
        IndexResponse response = client.prepareIndex(index, type, "14")
                                         .setSource(source).get();
        System.out.println("version: " + response.getVersion());
    }

    @Test
    public void testCreateBean() {
        BigdataProduct bp = new BigdataProduct("flume", "cloudera", 1.9f);

        JSONObject jsonObj = new JSONObject(bp);
        String source = jsonObj.toString();
        IndexResponse response = client.prepareIndex(index, type, "5")
                                         .setSource(source, XContentType.JSON).get();
        System.out.println("version: " + response.getVersion());
    }

    @Test
    public void testCreateBean2Map() throws Exception {
        BigdataProduct bp = new BigdataProduct("kafka", "LinkedIn", 2.0f);
        Map<String, Object> map = bean2Map2(bp);
        IndexResponse response = client.prepareIndex(index, type, "6")
                                         .setSource(map).get();
        System.out.println("version: " + response.getVersion());
    }

    @Test
    public void testCreateXContentBuilder() throws Exception {
        BigdataProduct bp = new BigdataProduct("spark", "cbkl", 2.4f);
        XContentBuilder builder = bean2Builder(bp);
        IndexResponse response = client.prepareIndex(index, type, "7")
                                         .setSource(builder).get();
        System.out.println("version: " + response.getVersion());
    }

    //局部修改
    @Test
    public void testUpdate() {
        String doc ="{\"version\": 2.2}";
        UpdateResponse response = client.prepareUpdate(index, type, "7")
                                          .setDoc(doc, XContentType.JSON).get();
        System.out.println("version: " + response.getVersion());
    }

    @Test
    public void testDelete() {
        DeleteResponse response = client.prepareDelete(index, type, "7").get();
        System.out.println("version: " + response.getVersion());
    }

    @Test
    public void testBulk() {
        String doc ="{\"version\": 2.2}";
        BulkResponse response = client.prepareBulk()
                     .add(client.prepareDelete(index, type, "6"))
                     .add(client.prepareUpdate(index, type, "5").setDoc(doc, XContentType.JSON))
                     .get();
        BulkItemResponse[] items = response.getItems();
        for (BulkItemResponse bir : items) {
            System.out.println("version: " + bir.getVersion());
        }
    }


    private Map<String, Object> bean2Map(BigdataProduct bp) throws Exception {
        Map<String, Object> map = new HashMap<String, Object>();

        Class<? extends BigdataProduct> clazz = bp.getClass();

        Field[] fields = clazz.getDeclaredFields();//获取所有的public的字段
        for (Field field : fields) {
            String key = field.getName();
//            field.setAccessible(true);//暴力反射
//            Object fieldValue = field.get(bp);
//            String methodName = "get" + (key.charAt(0) + "").toUpperCase() + key.substring(1);
//            Method method = clazz.getMethod(methodName);
//            Object fieldValue = method.invoke(bp);
            PropertyDescriptor pb = new PropertyDescriptor(key, clazz);
            Method rMethod = pb.getReadMethod();
            Object fieldValue = rMethod.invoke(bp);
            map.put(key, fieldValue);
        }

        return map;
    }

    private <T> Map<String, Object> bean2Map2(T obj) throws Exception {
        Map<String, Object> map = new HashMap<String, Object>();
        Class clazz = obj.getClass();
        Field[] fields = clazz.getDeclaredFields();//获取所有的public的字段
        PropertyDescriptor pb = null;
        for (Field field : fields) {
            String key = field.getName();
            pb = new PropertyDescriptor(key, clazz);
            Method rMethod = pb.getReadMethod();
            Object fieldValue = rMethod.invoke(obj);
            map.put(key, fieldValue);
        }
        return map;
    }

    private <T> XContentBuilder bean2Builder(T obj) throws Exception {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        Class clazz = obj.getClass();
        Field[] fields = clazz.getDeclaredFields();//获取所有的public的字段
        PropertyDescriptor pb = null;
        for (Field field : fields) {
            String key = field.getName();
            pb = new PropertyDescriptor(key, clazz);
            Method rMethod = pb.getReadMethod();
            Object fieldValue = rMethod.invoke(obj);
            builder.field(key, fieldValue);
        }
        builder.endObject();
        return builder;
    }


    @After
    public void cleanUp() {
        client.close();
    }
}
