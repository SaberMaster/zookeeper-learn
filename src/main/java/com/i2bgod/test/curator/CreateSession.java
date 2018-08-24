package com.i2bgod.test.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * @auther: Lyn
 * @data: 2018/8/24
 */
public class CreateSession {
    public static void main(String[] args) throws InterruptedException {
        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                "zookeeper1:2181",
                5000,
                3000,
                retry);
        client.start();
        TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
    }

    @Test
    public void createSessionFluent() throws InterruptedException {

        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("zookeeper1:2181")
                .sessionTimeoutMs(5000)
                .retryPolicy(retry)
                .connectionTimeoutMs(3000)
                .build();
        client.start();
        TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
    }


    @Test
    public void namespaceSession() throws InterruptedException {

        ExponentialBackoffRetry retry = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString("zookeeper1:2181")
                .sessionTimeoutMs(5000)
                .retryPolicy(retry)
                .connectionTimeoutMs(3000)
                .namespace("base")
                .build();
        client.start();
        TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
    }
}
