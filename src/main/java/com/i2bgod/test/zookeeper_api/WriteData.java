package com.i2bgod.test.zookeeper_api;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @auther: Lyn
 * @data: 2018/8/15
 */
public class WriteData {
    @Test
    public void writeDataSync() throws IOException, KeeperException, InterruptedException {

        String path = "/zk-writeData";

        CreateSession createSession = new CreateSession();

        ZooKeeper zooKeeper = createSession.doConnect();
        // create node with data
        zooKeeper.create(path,
                "123".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);

        zooKeeper.getData(path, true, null);

        Stat stat = zooKeeper.setData(path,
                "456".getBytes(),
                -1);
        System.out.println(stat.getCzxid() + ", " + stat.getMzxid() + "," + stat.getVersion());

        Stat stat2 = zooKeeper.setData(path,
                "456".getBytes(),
                stat.getVersion());
        System.out.println(stat2.getCzxid() + ", " + stat2.getMzxid() + "," + stat2.getVersion());

        try {
            zooKeeper.setData(path,
                    "456".getBytes(),
                    stat.getVersion());
        } catch (KeeperException e) {
            System.out.println("Error: " + e.code() + "," + e.getMessage());
        }

        TimeUnit.SECONDS.sleep(5);
    }

    class IStatCallback implements AsyncCallback.StatCallback{
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            if (0 == rc) {
                System.out.println("success");
            }
        }
    }


    @Test
    public void writeDataAsync() throws IOException, KeeperException, InterruptedException {

        String path = "/zk-writeData-async";

        CreateSession createSession = new CreateSession();

        ZooKeeper zooKeeper = createSession.doConnect();
        // create node with data
        zooKeeper.create(path,
                "123".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);

        zooKeeper.setData(path,
                "456".getBytes(),
                -1,
                new IStatCallback(),
                null);

        TimeUnit.SECONDS.sleep(5);
    }
}
