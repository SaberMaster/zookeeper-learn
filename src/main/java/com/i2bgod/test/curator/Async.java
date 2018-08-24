package com.i2bgod.test.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @auther: Lyn
 * @data: 2018/8/24
 */
public class Async {
    private static String zkUrl = "zookeeper1:2181";
    private static CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(zkUrl)
            .sessionTimeoutMs(5000)
            .retryPolicy(new ExponentialBackoffRetry(
                    1000,
                    3))
            .build();
    String path = "/zk-node";

    static CountDownLatch semaphore = new CountDownLatch(2);

    static ExecutorService tp = Executors.newFixedThreadPool(2);

    @Test
    public void createNode() throws Exception {
       client.start();
        System.out.println("Main thread: " + Thread.currentThread().getName());

        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .inBackground((client, event) -> {
                    System.out.println("event[code: "
                            + event.getResultCode()
                            + ", type:"
                            + event.getType()
                            + "]");

                    System.out.println("Thread of processResult:" + Thread.currentThread().getName());
                    semaphore.countDown();
                }, tp)
                .forPath(path, "init".getBytes());

        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .inBackground((client, event) -> {
                    System.out.println("event[code: "
                            + event.getResultCode()
                            + ", type:"
                            + event.getType()
                            + "]");

                    System.out.println("Thread of processResult:" + Thread.currentThread().getName());
                   semaphore.countDown();
                })
                .forPath(path, "init".getBytes());

        semaphore.await();
        tp.shutdown();
    }
}
