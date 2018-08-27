package com.i2bgod.test.curator.recipes;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * @auther: Lyn
 * @data: 2018/8/27
 */
public class MasterSelect {
    private static String zkUrl = "zookeeper1:2181";
    private static CuratorFramework client = CuratorFrameworkFactory.builder()
            .connectString(zkUrl)
            .sessionTimeoutMs(5000)
            .retryPolicy(new ExponentialBackoffRetry(
                    1000,
                    3))
            .build();
    String master_path = "/curator_recipes_master_path";

    @Test
    public void masterSelect() throws InterruptedException {
        client.start();
        LeaderSelector selector = new LeaderSelector(client,
                master_path,
                new LeaderSelectorListenerAdapter() {
                    @Override
                    public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
                        System.out.println("being master");
                        TimeUnit.SECONDS.sleep(2);
                        System.out.println("release master");
                    }
                }
        );
        selector.autoRequeue();
        selector.start();
        TimeUnit.SECONDS.sleep(20);
    }
}
