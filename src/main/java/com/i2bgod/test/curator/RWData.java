package com.i2bgod.test.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

/**
 * @auther: Lyn
 * @data: 2018/8/24
 */
public class RWData {
    private static String zkUrl = "zookeeper1:2181";
    private static CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(zkUrl)
            .sessionTimeoutMs(5000)
            .retryPolicy(new ExponentialBackoffRetry(
                    1000,
                    3))
            .build();
    String path = "/zk-node";


    @Test
    public void getData() throws Exception {

        client.start();
        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(path, "init".getBytes());

        System.out.println(new String(client.getData().forPath(path)));
    }

    @Test
    public void updateData() throws Exception {
        client.start();
        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(path, "init".getBytes());

        Stat stat = new Stat();

        client.getData().storingStatIn(stat).forPath(path);
        System.out.println("success set node for : " + path + ", new version: " +
        client.setData().withVersion(stat.getVersion()).forPath(path, "change".getBytes()).getVersion());

        try {
            client.setData().withVersion(stat.getVersion()).forPath(path);
        } catch (Exception e) {
            System.out.println("fail set node data to  " + e.getMessage());
        }
    }

}
