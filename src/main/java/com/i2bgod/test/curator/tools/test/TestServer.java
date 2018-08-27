package com.i2bgod.test.curator.tools.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;

import java.io.File;
import java.nio.file.Path;

/**
 * @auther: Lyn
 * @data: 2018/8/27
 */
public class TestServer {
    static String path = "/zookeeper";

    public static void main(String[] args) throws Exception {
        TestingServer server = new TestingServer(2181,
                new File("/tmp/zk/data"));
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .sessionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();

        client.start();
        System.out.println(client.getChildren().forPath(path));
        server.close();

    }
}
