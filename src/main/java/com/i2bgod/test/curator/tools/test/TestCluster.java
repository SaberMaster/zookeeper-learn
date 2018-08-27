package com.i2bgod.test.curator.tools.test;

import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.TestingZooKeeperServer;

import java.util.concurrent.TimeUnit;

/**
 * @auther: Lyn
 * @data: 2018/8/27
 */
public class TestCluster {
    public static void main(String[] args) throws Exception {
        TestingCluster cluster = new TestingCluster(3);
        cluster.start();
        TimeUnit.SECONDS.sleep(2);
        final TestingZooKeeperServer[] leader = {null};
        cluster.getServers().forEach((zs) -> {
            System.out.println(zs.getInstanceSpec().getServerId() + "-");
            System.out.println(zs.getQuorumPeer().getServerState() + "-");
            System.out.println(zs.getInstanceSpec().getDataDirectory().getAbsolutePath());

            if (zs.getQuorumPeer().getServerState().equals("leading")) {
               leader[0] = zs;
            }
        });
        System.out.println("leading:" + leader[0].getQuorumPeer().getServerState() );
        leader[0].kill();
        System.out.println("-- after leading kill:");
        cluster.getServers().forEach((zs) -> {
            System.out.println(zs.getInstanceSpec().getServerId() + "-");
            System.out.println(zs.getQuorumPeer().getServerState() + "-");
            System.out.println(zs.getInstanceSpec().getDataDirectory().getAbsolutePath());
        });
       cluster.stop();
    }
}
