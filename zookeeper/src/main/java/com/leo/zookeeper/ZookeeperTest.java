package com.leo.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZookeeperTest {

    ZooKeeper zkClient;

    @Before
    public void init() throws IOException {
        String connectStr = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        int sessionTimeout = 2000;
        zkClient = new ZooKeeper(connectStr, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getType() + "--" + watchedEvent.getPath());

                try {
                    List<String> children = zkClient.getChildren("/", true);
                    for (String child : children) {
                        System.out.println(child);
                    }
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Test
    public void createNode() throws KeeperException, InterruptedException {
        String nodeInfo = zkClient.create("/leo", "leo".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("nodeInfo = " + nodeInfo);
    }

    @Test
    public void exist() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/leo", false);
        System.out.println(stat == null ? "not exist" : "exist");
    }

    @Test
    public void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }

        Thread.sleep(Long.MAX_VALUE);
    }
}
