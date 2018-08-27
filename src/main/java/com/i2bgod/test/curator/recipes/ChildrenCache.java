package com.i2bgod.test.curator.recipes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * @auther: Lyn
 * @data: 2018/8/27
 */
public class ChildrenCache {
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
    public void childrenCacheListen() throws Exception {
        client.start();
        PathChildrenCache cache = new PathChildrenCache(client, path, true);
        cache.getListenable()
                .addListener((client, event) -> {
                   switch (event.getType()) {
                       case CHILD_ADDED:
                           System.out.println("CHILD_ADDED," + event.getData().getPath());
                           break;
                       case CHILD_UPDATED:
                           System.out.println("CHILD_UPDATED," + event.getData().getPath());
                           break;
                       case CHILD_REMOVED:
                           System.out.println("CHILD_REMOVED," + event.getData().getPath());
                           break;

                           default:
                               break;

                   }
                });
        cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

        client.create()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path);
        TimeUnit.SECONDS.sleep(1);

        client.create()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path + "/c1");

        TimeUnit.SECONDS.sleep(1);

        client.delete().forPath(path + "/c1");
        TimeUnit.SECONDS.sleep(1);

        client.delete().forPath(path);
        TimeUnit.SECONDS.sleep(3);


    }
}
