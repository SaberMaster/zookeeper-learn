package com.i2bgod.test.curator.recipes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * @auther: Lyn
 * @data: 2018/8/27
 */
public class NodeCacheCreate {
    private static String zkUrl = "zookeeper1:2181";
    private static CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(zkUrl)
            .sessionTimeoutMs(5000)
            .namespace("base")
            .retryPolicy(new ExponentialBackoffRetry(
                    1000,
                    3))
            .build();
    String path = "/zk-node/nodecache";


    @Test
    public void nodeCache() throws Exception {
       client.start();
       client.create()
               .creatingParentsIfNeeded()
               .withMode(CreateMode.EPHEMERAL)
               .forPath(path, "init".getBytes());

        final NodeCache cache = new NodeCache(client, path, false);
        cache.start(true);
        cache.getListenable()
                .addListener(() -> {
                    System.out.println("Node data update, new data:"
                            + new String(cache.getCurrentData().getData()));
                });

        client.setData().forPath(path, "update".getBytes());
        TimeUnit.SECONDS.sleep(5);
        client.delete()
                .deletingChildrenIfNeeded()
                .forPath(path);

        TimeUnit.SECONDS.sleep(5);
    }
}
