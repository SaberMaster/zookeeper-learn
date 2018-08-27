package com.i2bgod.test.curator.recipes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.junit.Test;

/**
 * @auther: Lyn
 * @data: 2018/8/27
 */
public class Timer {
    private static String zkUrl = "zookeeper1:2181";
    private static CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(zkUrl)
            .sessionTimeoutMs(5000)
            .retryPolicy(new ExponentialBackoffRetry(
                    1000,
                    3))
            .build();
    String distatom_path = "/curator_recipes_distatomicint_path";

    @Test
    public void counter() throws Exception {
        client.start();
        DistributedAtomicInteger atomicInteger = new DistributedAtomicInteger(client, distatom_path, new RetryNTimes(3, 1000));
        AtomicValue<Integer> rc = atomicInteger.add(8);
        System.out.println(atomicInteger.get().postValue());
        System.out.println("Result: " + rc.succeeded());

    }

}
