package com.i2bgod.test.zookeeper_api;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * @auther: Lyn
 * @data: 2018/8/15
 */
public class NodeCreate {
    private void createNode(ZooKeeper zooKeeper,String path, String data, ArrayList<ACL> ids, CreateMode mode) throws KeeperException, InterruptedException {
        String path1 = zooKeeper.create(path,
                data.getBytes(),
                ids,
                mode);

        System.out.println("Success create znode:" + path1);
    }

    @Test
    public void createNodeSync() throws IOException, KeeperException, InterruptedException {
        CreateSession createSession = new CreateSession();
        ZooKeeper zooKeeper = createSession.doConnect("zookeeper1:2181");
        // create ephemeral node
        createNode(zooKeeper,
                "/zk-test-ephemeral-",
                "",
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);
        // create seq
        createNode(zooKeeper,
                "/zk-test-ephemeral-",
                "",
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
        TimeUnit.SECONDS.sleep(3);
    }


    private void createNodeAsnyc(ZooKeeper zooKeeper, String path, String data, ArrayList<ACL> ids, CreateMode mode, AsyncCallback.StringCallback cb, Object ctx){
        zooKeeper.create(path,
                data.getBytes(),
                ids,
                mode,
                cb,
                ctx);
    }

    class IStringCallback implements AsyncCallback.StringCallback {
        public void processResult(int rc, String path, Object ctx, String name) {
            System.out.println("create path result: [" + rc + ", " + path + ", " + ctx + ", real path name: " + name);
        }
    }

    @Test
    public void createNodeASync() throws IOException, InterruptedException {
        CreateSession createSession = new CreateSession();
        ZooKeeper zooKeeper = createSession.doConnect("zookeeper1:2181");
        // create ephemeral node
        createNodeAsnyc(zooKeeper,
                "/zk-test-ephemeral-",
                "",
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                new IStringCallback(),
                "I am context");
        // create ephemeral node
        createNodeAsnyc(zooKeeper,
                "/zk-test-ephemeral-",
                "",
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                new IStringCallback(),
                "I am context");
        // create seq
        createNodeAsnyc(zooKeeper,
                "/zk-test-ephemeral-",
                "",
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL,
                new IStringCallback(),
                "I am context");
        TimeUnit.SECONDS.sleep(3);
    }
}
