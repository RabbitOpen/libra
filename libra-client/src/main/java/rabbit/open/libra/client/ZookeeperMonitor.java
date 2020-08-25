package rabbit.open.libra.client;

import org.I0Itec.zkclient.IZkStateListener;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * zk事件监听
 * @author xiaoqianbin
 * @date 2020/8/16
 **/
public abstract class ZookeeperMonitor {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * zk交互对象
     **/
    protected RegistryHelper helper;

    /**
     * 获取zk辅助类配置
     * @author xiaoqianbin
     * @date 2020/8/16
     **/
    protected abstract RegistryConfig getConfig();

    /**
     * 获取zk交互对象实例
     * @author xiaoqianbin
     * @date 2020/8/16
     **/
    public RegistryHelper getRegistryHelper() {
        return helper;
    }

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
     * @author xiaoqianbin
     * @date 2020/8/16
     **/
    protected void init() {
        RegistryConfig config = getConfig();
        helper = new RegistryHelper(config.getHosts(), config.getNamespace());
        helper.init();
        getRegistryHelper().subscribeStateChanges(new IZkStateListener() {

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
