package com.leo.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;

public class DistributeServer {
    private ZooKeeper zk;

    public static void main(String[] args) throws Exception {
        DistributeServer server = new DistributeServer();
        server.connect();
        server.register(args[0]);
        server.business(args[0]);
    }

    private void connect() throws IOException {
        String connectStr = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        int sessionTimeout = 2000;
        zk = new ZooKeeper(connectStr, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    private void register(String host) throws Exception {
        String node = zk.create("/servers/server", host.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(host + "is  online" + node);
    }

    private void business(String host) throws Exception {
        System.out.println(host + "is working ...");
        Thread.sleep(Long.MAX_VALUE);
    }
}
