package com.hellozjf.test.hbaseclient;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class BeanConfig {

    @Bean
    public Connection connection() throws IOException {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "arask179");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.client.keyvalue.maxsize", String.valueOf(10 * 1024 * 1024));
        config.set("hbase.rpc.timeout ", String.valueOf(2000));
        config.set("hbase.client.retries.number ", "3");
        return ConnectionFactory.createConnection(config);
    }
}
