package rabbit.open.libra.client.utils;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @author xiaoqianbin
 * @date 2020/7/14
 **/
public class Test {

    private static Logger logger = LoggerFactory.getLogger(Test.class);

    public static void main(String[] args) throws IOException, InterruptedException {

        ZkClient client = new ZkClient("10.222.16.82:2181");
        client.subscribeStateChanges(new IZkStateListener() {

            @Override
            public void handleStateChanged(Watcher.Event.KeeperState keeperState) throws Exception {
                logger.info("handleStateChanged {}", keeperState);
            }

            @Override
            public void handleNewSession() throws Exception {
                logger.info("new session created");

            }

            @Override
            public void handleSessionEstablishmentError(Throwable throwable) throws Exception {
                logger.error(throwable.getMessage(), throwable);
            }
        });
        Thread.sleep(1000);
        client.create("/libra/xx", null, CreateMode.EPHEMERAL);
        client.subscribeChildChanges("/libra", new IZkChildListener() {
            @Override
            public void handleChildChange(String s, List<String> list) throws Exception {
                logger.info("{} child changed {}", s, list);
            }
        });

        System.in.read();
    }
}
