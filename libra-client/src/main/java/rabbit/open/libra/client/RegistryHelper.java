package rabbit.open.libra.client;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
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

    /**
     * 递归创建根节点
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    private void createRootPath() {
        String[] nodes = rootPath.split("/");
        String path = "";
        for (String node : nodes) {
            if ("".equals(node.trim())) {
                continue;
            }
            path = path + "/" + node;
            if (client.exists(path)) {
                return;
            }
            try {
                client.create(path, null, CreateMode.PERSISTENT);
            } catch (Exception e) {
                // TO DO: ignore all
            }
        }
    }

    /**
     * 注册任务执行器
     * @author  xiaoqianbin
     * @date    2020/7/11
     **/
    private void registerExecutor() {
        try {
            executorName = InetAddress.getLocalHost().getHostName();
            replaceNode("/executors/" + executorName, null, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }

    /**
     * 创建executor目录
     * @param   nodePath
     * @author  xiaoqianbin
     * @date    2020/7/11
     **/
    private String createPersistentNode(String nodePath) {
        try {
            return createNode(nodePath, null, CreateMode.PERSISTENT);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
            return rootPath + nodePath;
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
    public String replaceNode(String relative, Object data, CreateMode mode) {
        removeNode(relative);
        return createNode(relative, data, mode);
    }

    /**
     * 创建节点
     * @param	relative
	 * @param	data
	 * @param	mode
     * @author  xiaoqianbin
     * @date    2020/7/11
     **/
    public String createNode(String relative, Object data, CreateMode mode) {
        if (client.exists(rootPath + relative)) {
            return rootPath + relative;
        }
        return client.create(rootPath + relative, data, mode);
    }

    /**
     * 初始化目录节点
     * @author  xiaoqianbin
     * @date    2020/7/14
     **/
    @PostConstruct
    public void init() {
        client = new ZkClient(hosts);
        createRootPath();
        createPersistentNode("/executors");
        createPersistentNode("/tasks");
        createPersistentNode("/tasks/meta");
        createPersistentNode("/tasks/meta/system");
        createPersistentNode("/tasks/meta/users");
        createPersistentNode("/tasks/execution");
        createPersistentNode("/tasks/execution/users");
        createPersistentNode("/tasks/execution/system");
        // 处于执行中的任务
        createPersistentNode("/tasks/execution/schedule");
        registerExecutor();
    }

    /**
     * 销毁客户端
     * @author  xiaoqianbin
     * @date    2020/7/11
     **/
    @PreDestroy
    public synchronized void destroy() {
        if (!destroyed) {
            removeNode("/executors/" + executorName);
            logger.info("zookeeper client is closing.........");
            client.close();
            logger.info("zookeeper client is closed!");
        }
    }

    /**
     * 注册任务节点
     * @param	name
     * @param	data
	 * @param	system
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    public void registerTaskMeta(String name, Object data, boolean system) {
        String metaPath = "/tasks/meta/system";
        if (!system) {
            metaPath = "/tasks/meta/users";
        }
        replaceNode(metaPath + "/" + name, data, CreateMode.PERSISTENT);
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

    public String getRootPath() {
        return rootPath;
    }
}
