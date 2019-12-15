package com.example.esconnection.util;



import com.example.esconnection.conf.ConfigurationManager;
import com.example.esconnection.conf.Constants;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetSocketAddress;
import java.util.LinkedList;

//参考连接池pool操作
public class TransportUtil {


    private static LinkedList<TransportClient> pool = new LinkedList<TransportClient>();
    static {
        Settings settings = Settings.builder()
                                    .put("cluster.name", ConfigurationManager.getProperties(Constants.ES_CLUSTER_NAME))
                                    .build();
        TransportClient client = new PreBuiltTransportClient(settings);
        String hostAndPorts = ConfigurationManager.getProperties(Constants.ES_CLUSTER_HOST_PORT);
        for (String hostAndPort : hostAndPorts.split(",")) {
            String host = hostAndPort.split(":")[0];
            int port = Integer.valueOf(hostAndPort.split(":")[1]);
            client.addTransportAddress(new TransportAddress(new InetSocketAddress(host, port)));
        }
        pool.push(client);
    }

    public static TransportClient getClient() {
        while(pool.isEmpty()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return pool.poll();
    }

    public static void release(TransportClient client) {
        pool.push(client);
    }
}