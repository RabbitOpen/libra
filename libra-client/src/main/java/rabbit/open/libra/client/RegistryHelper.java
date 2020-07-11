package rabbit.open.libra.client;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import java.net.InetAddress;

/**
 * zk client 辅助类
 * @author xiaoqianbin
 * @date 2020/7/10
 **/
public class RegistryHelper {

    private Logger logger = LoggerFactory.getLogger(getClass());

    // zk地址
    @Value("${zookeeper.hosts.url}")
    private String hosts;

    // 监控根节点
    @Value("${libra.monitor.root-path:/libra/root}")
    private String rootPath;

    private ZkClient client;

    private boolean destroyed = false;

    // 任务节点名字
    private String executorName;

    @PostConstruct
    public void init() {
        client = new ZkClient(hosts);
        createExecutorsNode();
        registerExecutor();
    }

    /**
     * 注册任务执行器
     * @author  xiaoqianbin
     * @date    2020/7/11
     **/
    private void registerExecutor() {
        try {
            executorName = InetAddress.getByName("localhost").getHostName();
            createNode("/executors/" + executorName, null, CreateMode.PERSISTENT);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }

    /**
     * 创建executor目录
     * @author  xiaoqianbin
     * @date    2020/7/11
     **/
    private void createExecutorsNode() {
        try {
            createNode("/executors", null, CreateMode.PERSISTENT);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }

    /**
     * 替换已经存在的node
     * @param	relative
	 * @param	data
	 * @param	mode
     * @author  xiaoqianbin
     * @date    2020/7/11
     **/
    public void replaceNode(String relative, Object data, CreateMode mode) {
        removeNode(relative);
        createNode(relative, data, mode);
    }

    /**
     * 创建节点
     * @param	relative
	 * @param	data
	 * @param	mode
     * @author  xiaoqianbin
     * @date    2020/7/11
     **/
    public void createNode(String relative, Object data, CreateMode mode) {
        if (client.exists(rootPath + relative)) {
            return;
        }
        client.create(rootPath + relative, data, mode);
    }

    /**
     * 销毁客户端
     * @author  xiaoqianbin
     * @date    2020/7/11
     **/
    public synchronized void destroy() {
        if (!destroyed) {
            removeNode("/executors/" + executorName);
            logger.info("zookeeper client is closing.........");
            client.close();
            logger.info("zookeeper client is closed!");
        }
    }

    /**
     * 删除节点
     * @param	relative    相对于根节点的节点路径
     * @author  xiaoqianbin
     * @date    2020/7/11
     **/
    private void removeNode(String relative) {
        if (client.exists(rootPath + relative)) {
            client.delete(rootPath + relative);
        }
    }

    public ZkClient getClient() {
        return client;
    }
}
