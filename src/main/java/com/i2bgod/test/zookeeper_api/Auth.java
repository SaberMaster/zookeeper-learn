package com.i2bgod.test.zookeeper_api;

import org.apache.zookeeper.*;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @auther: Lyn
 * @data: 2018/8/15
 */
public class Auth {
    final static String PATH = "/zk-auth-test";

    @Test
    public void basicCon() throws IOException, KeeperException, InterruptedException {
        CreateSession createSession = new CreateSession();
        ZooKeeper zooKeeper = createSession.doConnect();

        zooKeeper.addAuthInfo("digest",
                "foo:true".getBytes());

        String path = zooKeeper.create(PATH,
                "init".getBytes(),
                ZooDefs.Ids.CREATOR_ALL_ACL,
                CreateMode.EPHEMERAL);
    }

    @Test
    public void authGetErr() throws InterruptedException, IOException, KeeperException {
        basicCon();
        CreateSession createSession = new CreateSession();
        ZooKeeper zooKeeper = createSession.doConnect();
        zooKeeper.getData(PATH, false, null);

    }


    @Test
    public void authGetOk() throws InterruptedException, IOException, KeeperException {
        basicCon();
        CreateSession createSession = new CreateSession();
        ZooKeeper zooKeeper = createSession.doConnect();
        zooKeeper.addAuthInfo("digest",
                "foo:true".getBytes());
        System.out.println(zooKeeper.getData(PATH, false, null));

        ZooKeeper zooKeeper2 = createSession.doConnect();
        zooKeeper.addAuthInfo("digest",
                "foo:false".getBytes());
        System.out.println(zooKeeper2.getData(PATH, false, null));

    }


    @Test
    public void authDel() throws IOException, KeeperException, InterruptedException {
        CreateSession createSession = new CreateSession();
        ZooKeeper zooKeeper = createSession.doConnect();

        zooKeeper.addAuthInfo("digest",
                "foo:true".getBytes());

        String path = zooKeeper.create(PATH,
                "init".getBytes(),
                ZooDefs.Ids.CREATOR_ALL_ACL,
                CreateMode.PERSISTENT);

        String path2 = PATH + "/child";
        zooKeeper.create(path2,
                "init".getBytes(),
                ZooDefs.Ids.CREATOR_ALL_ACL,
                CreateMode.EPHEMERAL);

        try {
            ZooKeeper zooKeeper2 = createSession.doConnect();

            zooKeeper2.delete(path2, -1);
        } catch (Exception e) {
            System.out.println("del node fail:" + e.getMessage());
        }

        ZooKeeper zookeeper3 = createSession.doConnect();
        zookeeper3.addAuthInfo("digest",
                "foo:true".getBytes());
        zookeeper3.delete(path2, -1);
        System.out.println("del node success:" + path2);



        ZooKeeper zookeeper4 = createSession.doConnect();
        zookeeper4.delete(PATH, -1);
        System.out.println("del node success:" + PATH);


    }
}
