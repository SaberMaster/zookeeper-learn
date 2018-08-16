package com.i2bgod.test.zookeeper_api;

import org.apache.zookeeper.*;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @auther: Lyn
 * @data: 2018/8/15
 */
public class NodeExist {


    class NodeWatcher implements Watcher {
        public void process(WatchedEvent event) {
            try {
                if (Event.KeeperState.SyncConnected == event.getState()) {
                    if (Event.EventType.None == event.getType() &&
                            null == event.getPath()) {
                        CreateSession.counter.countDown();
                        // watch node data change event
                    } else if (Event.EventType.NodeCreated == event.getType()) {
                        System.out.println("Node(" + event.getPath() + ") created");
                    } else  if (Event.EventType.NodeDeleted == event.getType()) {
                        System.out.println("Node(" + event.getPath() + ") deleted");
                    }
                    else if (Event.EventType.NodeDataChanged == event.getType()) {
                        System.out.println("Node(" + event.getPath() + ") datachange");
                    }
                    // add watcher
                    CreateSession.zooKeeper.exists(event.getPath(), true);
                }
            } catch (Exception e) {

            }
        }
    }


    @Test
    public void nodeExist() throws IOException, KeeperException, InterruptedException {
        String path = "/zk-nodeExist";
        CreateSession createSession = new CreateSession();
        ZooKeeper zooKeeper = createSession.doConnect(new NodeWatcher());

        zooKeeper.exists(path, true);

        // create parent node
        zooKeeper.create(path,
                "".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        zooKeeper.setData(path,
                "123".getBytes(),
                -1);

        // create child node
        zooKeeper.create(path + "/c1",
                "".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        zooKeeper.delete(path + "/c1", -1);

        zooKeeper.delete(path, -1);

        TimeUnit.SECONDS.sleep(5);
    }
}
