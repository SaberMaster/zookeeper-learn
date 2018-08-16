package com.i2bgod.test.zookeeper_api;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @auther: Lyn
 * @data: 2018/8/15
 */
public class CreateSession implements Watcher {
    public static CountDownLatch counter =  new CountDownLatch(1);
    public static ZooKeeper zooKeeper = null;

    public ZooKeeper doConnect() throws IOException{
        return zooKeeper = doConnect("zookeeper1:2181", new CreateSession());
    }

    public ZooKeeper doConnect(Watcher watcher) throws IOException{
        return zooKeeper = doConnect("zookeeper1:2181", watcher);
    }

    public ZooKeeper doConnect(String connectString) throws IOException{
        return zooKeeper = doConnect(connectString, new CreateSession());
    }


    public ZooKeeper doConnect(String connectString, Watcher watcher) throws IOException{
        zooKeeper = new ZooKeeper(connectString,
                5000,
                watcher);
        System.out.println(zooKeeper.getState());
        try {
            counter.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("zookeeper session established");
        return zooKeeper;
    }

    public void doConnectWithSIGAndPwd(String connectString) throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(connectString,
                5000,
                new CreateSession());
        System.out.println(zooKeeper.getState());
        long sessionId = zooKeeper.getSessionId();
        byte[] sessionPasswd = zooKeeper.getSessionPasswd();


        // error sessionId and passwd
        zooKeeper = new ZooKeeper(connectString,
                5000,
                new CreateSession(),
                1l,
                "test".getBytes());
        System.out.println(zooKeeper.getState());

        // correct
        zooKeeper = new ZooKeeper(connectString,
                5000,
                new CreateSession(),
                sessionId,
                sessionPasswd);
        System.out.println(zooKeeper.getState());

        Thread.sleep(Integer.MAX_VALUE);

    }

    public void process(WatchedEvent watchedEvent) {

        System.out.println("Receive watched event: " + watchedEvent);

        if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
            counter.countDown();
        }
    }

    @Test
    public void connect() throws IOException {
        CreateSession createSession = new CreateSession();
        createSession.doConnect("zookeeper1:2181");
    }

    @Test
    public void connectReuse() throws IOException, InterruptedException {
        CreateSession createSession = new CreateSession();
        createSession.doConnectWithSIGAndPwd("zookeeper1:2181");
    }
}
