package com.i2bgod.test.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;
import sun.misc.Cleaner;

import java.util.concurrent.TimeUnit;

/**
 * @auther: Lyn
 * @data: 2018/8/24
 */
public class CDNode {
   private static String zkUrl = "zookeeper1:2181";
   private static CuratorFramework client = CuratorFrameworkFactory.builder()
           .connectString(zkUrl)
           .sessionTimeoutMs(5000)
           .retryPolicy(new ExponentialBackoffRetry(
                   1000,
                   3))
           .build();
   String path = "/zk-node/c1";

   @Test
   public void createNode() throws Exception {
      client.start();
      client.create()
              .creatingParentsIfNeeded()
              .withMode(CreateMode.EPHEMERAL)
              .forPath(path, "init".getBytes());
      TimeUnit.SECONDS.sleep(5);
   }

   @Test
   public void deleteNode() throws Exception {
      client.start();
      client.create()
              .creatingParentsIfNeeded()
              .withMode(CreateMode.EPHEMERAL)
              .forPath(path, "init".getBytes());

      TimeUnit.SECONDS.sleep(5);
      Stat stat = new Stat();

      client.getData()
              .storingStatIn(stat)
              .forPath(path);
      client.delete()
              .deletingChildrenIfNeeded()
              .withVersion(stat.getVersion())
              .forPath(path);
   }

}
