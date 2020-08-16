package rabbit.open.libra.client;

import org.I0Itec.zkclient.IZkStateListener;
import org.apache.zookeeper.Watcher;

/**
 * zk事件监听
 * @author xiaoqianbin
 * @date 2020/8/16
 **/
public abstract class ZkListener {

    /**
     * 获取zk辅助类
     * @author xiaoqianbin
     * @date 2020/8/16
     **/
    protected abstract RegistryHelper getHelper();

    /**
     * zk失联
     * @author xiaoqianbin
     * @date 2020/8/16
     **/
    protected abstract void onZookeeperDisconnected();

    /**
     * ZK连上了
     * @author xiaoqianbin
     * @date 2020/8/16
     **/
    protected abstract void onZookeeperConnected();

    /**
     * 初始化
     * @author  xiaoqianbin
     * @date    2020/8/16
     **/
    protected void init() {
        getHelper().subscribeStateChanges(new IZkStateListener() {

            @Override
            public void handleStateChanged(Watcher.Event.KeeperState keeperState) {
                switch (keeperState) {
                    case Disconnected:
                        onZookeeperDisconnected();
                        break;
                    case SyncConnected:
                        onZookeeperConnected();
                        break;
                    default:
                        break;
                }
            }

            @Override
            public void handleNewSession() {
                // TO DO： ignore
            }

            @Override
            public void handleSessionEstablishmentError(Throwable throwable) {
                // TO DO： ignore
            }
        });
    }
}
