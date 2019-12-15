package com.example.esconnection;

import com.example.esconnection.util.TransportUtil;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Map;

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
        GetResponse response = client.prepareGet(index, type, "1").get();
        Map<String, Object> source = response.getSource();
        System.out.println(source);
    }

    @Test
    public void testCreateJSON() {
        String json ="{\"name\": \"hbase\", \"author\": \"oldli\", \"version\": 1.4}";
        IndexResponse response = client.prepareIndex(index, type, "3")
                .setSource(json, XContentType.JSON).get();
        System.out.println("version: " + response.getVersion());
    }
}
