package com.leo.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistributeClient {

    private ZooKeeper zk;

    public static void main(String[] args) throws Exception {
        DistributeClient client = new DistributeClient();

        client.connect();
        client.getChildren();
        client.business();
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren("/servers", true);
        List<String> nodeList = new ArrayList<String>();
        for (String child : children) {
            byte[] data = zk.getData("/servers/" + child, false, null);
            nodeList.add(new String(data));
        }
        System.out.println(nodeList);
    }

    private void connect() throws IOException {
        String connectStr = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        int sessionTimeout = 2000;
        zk = new ZooKeeper(connectStr, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                try {
                    getChildren();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

}
