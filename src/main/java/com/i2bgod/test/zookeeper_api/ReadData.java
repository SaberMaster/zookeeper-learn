package com.i2bgod.test.zookeeper_api;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @auther: Lyn
 * @data: 2018/8/15
 */
public class ReadData implements Watcher {

    public void process(WatchedEvent event) {
        if (Event.KeeperState.SyncConnected == event.getState()) {
            if (Event.EventType.None == event.getType() &&
                    null == event.getPath()) {
                CreateSession.counter.countDown();
                // watch node children change event
            } else if (Event.EventType.NodeChildrenChanged == event.getType()) {
                try {
                    // get changed children
                    System.out.println("ReGet Child: " + CreateSession.zooKeeper.getChildren(event.getPath(), true));
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void getChildrenSync() throws IOException, KeeperException, InterruptedException {
       String path = "/zk-getChild";
        CreateSession createSession = new CreateSession();
        ZooKeeper zooKeeper = createSession.doConnect(new ReadData());
        // create parent node
        zooKeeper.create(path,
                "".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        // create child node
        zooKeeper.create(path + "/c1",
                "".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);

        // sync
        List<String> children = zooKeeper.getChildren(path, true);
        System.out.println(children);

        // create child node
        zooKeeper.create(path + "/c2",
                "".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);

        TimeUnit.SECONDS.sleep(5);

    }

    class IChildren2Callback implements AsyncCallback.Children2Callback {

        public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
            System.out.println("get children znode result: [response code: " + rc + ", param path: " + path + ", ctx: " + ctx + ", children list: " + children + ", stat: " + stat);
        }
    }


    @Test
    public void getChildrenASync() throws IOException, KeeperException, InterruptedException {
        String path = "/zk-getChild-async";
        CreateSession createSession = new CreateSession();
        ZooKeeper zooKeeper = createSession.doConnect(new ReadData());
        // create parent node
        zooKeeper.create(path,
                "".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        // create child node
        zooKeeper.create(path + "/c1",
                "".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);

        // async
        zooKeeper.getChildren(path, true, new IChildren2Callback(), null);

        // create child node
        zooKeeper.create(path + "/c2",
                "".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);

        TimeUnit.SECONDS.sleep(5);

    }

    private static Stat stat = new Stat();

    class GetDataWatcher implements Watcher{
        public void process(WatchedEvent event) {
            if (Event.KeeperState.SyncConnected == event.getState()) {
                if (Event.EventType.None == event.getType() &&
                        null == event.getPath()) {
                    CreateSession.counter.countDown();
                    // watch node data change event
                } else if (Event.EventType.NodeDataChanged == event.getType()) {
                    try {
                        // get changed data
                        System.out.println("ReGet Child: " + new String(CreateSession.zooKeeper.getData(event.getPath(), true, stat)));
                        System.out.println(stat.getCzxid() + ", " + stat.getMzxid() + "," + stat.getVersion());
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    @Test
    public void getDataSync() throws IOException, KeeperException, InterruptedException {
       String path = "/zk-getData";

        CreateSession createSession = new CreateSession();
        ZooKeeper zooKeeper = createSession.doConnect(new GetDataWatcher());
        // create node with data
        zooKeeper.create(path,
                "123".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);

        System.out.println(new String(zooKeeper.getData(path, true, stat)));
        System.out.println(stat.getCzxid() + ", " + stat.getMzxid() + "," + stat.getVersion());

        zooKeeper.setData(path, "123".getBytes(), -1);

        TimeUnit.SECONDS.sleep(5);
    }


    class IDataCallback implements AsyncCallback.DataCallback {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            System.out.println(rc + ", " + path + ", " + new String(data));
            System.out.println(stat.getCzxid() + ", " + stat.getMzxid() + "," + stat.getVersion());
        }
    }

    class GetDataAsyncWatcher implements Watcher{
        public void process(WatchedEvent event) {
            if (Event.KeeperState.SyncConnected == event.getState()) {
                if (Event.EventType.None == event.getType() &&
                        null == event.getPath()) {
                    CreateSession.counter.countDown();
                    // watch node data change event
                } else if (Event.EventType.NodeDataChanged == event.getType()) {
                    // get changed data
                    CreateSession.zooKeeper.getData(event.getPath(), true, new IDataCallback(), null);
                }
            }
        }
    }

    @Test
   public void getDataAsync() throws IOException, KeeperException, InterruptedException {
        String path = "/zk-getDataAsync";

        CreateSession createSession = new CreateSession();
        ZooKeeper zooKeeper = createSession.doConnect(new GetDataAsyncWatcher());
        // create node with data
        zooKeeper.create(path,
                "123".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL);

        // async set data
        zooKeeper.getData(path, true, new IDataCallback(), null);
        zooKeeper.setData(path, "123".getBytes(), -1);
        TimeUnit.SECONDS.sleep(5);
    }
}
