package com.i2bgod.test.curator.recipes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @auther: Lyn
 * @data: 2018/8/27
 */
public class Lock {

    @Test
    public void generateOrder() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        IntStream.range(0, 10)
                .forEach((i) -> {
                    new Thread(() -> {
                        try {
                            countDownLatch.await();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
//                        synchronized (this) {
                            SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss | SSS");
                            String orderNo = sdf.format(new Date());
                            System.out.println("order:" + orderNo);
                            try {
                                TimeUnit.MILLISECONDS.sleep(20);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
//                        }
                    }).start();
                });
        countDownLatch.countDown();
        TimeUnit.SECONDS.sleep(5);
    }

    private static String zkUrl = "zookeeper1:2181";
    private static CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(zkUrl)
            .sessionTimeoutMs(5000)
            .retryPolicy(new ExponentialBackoffRetry(
                    1000,
                    3))
            .build();
    String lock_path = "/curator_recipes_lock_path";
    @Test
    public void generateOrderWithLock() throws InterruptedException {
       client.start();
        InterProcessMutex mutex = new InterProcessMutex(client, lock_path);
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        IntStream.range(0, 10)
                .forEach((i) -> {
                    new Thread(() -> {
                        try {
                            countDownLatch.await();
                            mutex.acquire();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss | SSS");
                        String orderNo = sdf.format(new Date());
                        System.out.println("order:" + orderNo);
                        try {
                            mutex.release();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }).start();
                });
        countDownLatch.countDown();
        TimeUnit.SECONDS.sleep(5);
    }
}
