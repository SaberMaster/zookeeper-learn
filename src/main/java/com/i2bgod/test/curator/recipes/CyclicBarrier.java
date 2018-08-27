package com.i2bgod.test.curator.recipes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

import java.util.concurrent.*;
import java.util.stream.IntStream;

/**
 * @auther: Lyn
 * @data: 2018/8/27
 */
public class CyclicBarrier {
    public static java.util.concurrent.CyclicBarrier barrier = new java.util.concurrent.CyclicBarrier(3);

    class Runner implements Runnable {
        private String name;
        public Runner(String name) {
           this.name = name;
        }
        @Override
        public void run() {
            System.out.println(name + "prepare");
            try {
                barrier.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
            System.out.println(name + "run");
        }
    }

    @Test
    public void run() {
        ExecutorService threadPool = Executors.newFixedThreadPool(3);
        threadPool.submit(new Thread(new Runner("1")));
        threadPool.submit(new Thread(new Runner("2")));
        threadPool.submit(new Thread(new Runner("3")));
        threadPool.shutdown();
    }


    private static String zkUrl = "zookeeper1:2181";
    private static CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(zkUrl)
            .sessionTimeoutMs(5000)
            .retryPolicy(new ExponentialBackoffRetry(
                    1000,
                    3))
            .build();
    String barrier_path = "/curator_recipes_barrier_path";
    static DistributedBarrier distBarrier;

    @Test
    public void runDist() {
        IntStream.range(0, 4)
                .forEach((i) -> {
                    new Thread(() -> {
                        CuratorFramework client = CuratorFrameworkFactory.builder()
                                .connectString(zkUrl)
                                .sessionTimeoutMs(5000)
                                .retryPolicy(new ExponentialBackoffRetry(
                                        1000,
                                        3))
                                .build();
                        client.start();
                        distBarrier = new DistributedBarrier(client, barrier_path);
                        System.out.println(Thread.currentThread().getName() + "init");
                        try {
                            distBarrier.setBarrier();
                            distBarrier.waitOnBarrier();
                            System.out.println("start...");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }).start();
                });
        try {
            TimeUnit.SECONDS.sleep(2);
            distBarrier.removeBarrier();
            TimeUnit.SECONDS.sleep(2);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void distBarrRun() throws InterruptedException {

        IntStream.range(0, 5)
                .forEach((i) -> {
                   new Thread(() -> {
                       CuratorFramework client = CuratorFrameworkFactory.builder()
                               .connectString(zkUrl)
                               .sessionTimeoutMs(5000)
                               .retryPolicy(new ExponentialBackoffRetry(
                                       1000,
                                       3))
                               .build();
                       client.start();
                       DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(client,
                               barrier_path, 5);
                       try {
                           TimeUnit.SECONDS.sleep(Math.round(Math.random() * 3));
                           System.out.println(Thread.currentThread().getName() + "init");
                           barrier.enter();
                           System.out.println("start");
                           TimeUnit.SECONDS.sleep(Math.round(Math.random() * 3));
                           barrier.leave();
                           System.out.println("exit");

                       } catch (InterruptedException e) {
                           e.printStackTrace();
                       } catch (Exception e) {
                           e.printStackTrace();
                       }
                   }).start();
                });
        TimeUnit.SECONDS.sleep(5);
    }
}
