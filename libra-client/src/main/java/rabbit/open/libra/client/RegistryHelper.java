package rabbit.open.libra.client;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rabbit.open.libra.client.exception.LibraException;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * zk client 辅助类
 * @author xiaoqianbin
 * @date 2020/7/10
 **/
public class RegistryHelper {

    private Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 用户任务meta信息路径
     **/
    public static final String META_TASKS = "/meta/tasks";

    /**
     * 调度任务meta信息路径
     **/
    public static final String META_SCHEDULER = "/meta/scheduler";

    /**
     * dag节点
     **/
    public static final String GRAPHS = "/graphs";

    // zk地址
    private String hosts;

    // 监控根节点
    private String namespace;

    private ZkClient client;

    private boolean destroyed = false;

    // 任务节点名字
    private String executorName;

    public RegistryHelper(String hosts, String namespace) {
        this.hosts = hosts;
        this.namespace = namespace;
    }

    /**
     * 递归创建根节点
     * @author xiaoqianbin
     * @date 2020/7/13
     **/
    private void createNamespace() {
        createNode(namespace, null, CreateMode.PERSISTENT);
    }

    /**
     * 递归创建永久节点
     * @param	relativePath
     * @author  xiaoqianbin
     * @date    2020/7/16
     **/
    public void createNode(String relativePath) {
        createNode(namespace + relativePath, null, CreateMode.PERSISTENT);
    }

    /**
     * 递归创建永久节点
     * @param	relativePath
     * @param	data            数据
     * @author  xiaoqianbin
     * @date    2020/7/16
     **/
    public void createPersistNode(String relativePath, Object data) {
        createNode(namespace + relativePath, data, CreateMode.PERSISTENT);
    }

    /**
     * 递归创建节点
     * @param fullPath
     * @param data
     * @param mode
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    private String createNode(String fullPath, Object data, CreateMode mode) {
        String sep = Task.SP;
        String[] nodes = fullPath.split(sep);
        StringBuilder path = new StringBuilder();
        boolean createError = false;
        for (String node : nodes) {
            if ("".equals(node.trim())) {
                continue;
            }
            path.append(sep).append(node);
            createError = createPathNode(fullPath, data, mode, path);
        }
        if (createError && !client.exists(fullPath)) {
            // 如果发生错误，节点又不存在
            throw new LibraException(String.format("path[%s] create failed", fullPath));
        }
        return fullPath;
    }

    /**
     * 创建path (fullPath上的子节点)
     * @param	fullPath
	 * @param	data
	 * @param	mode
	 * @param	path
     * @author  xiaoqianbin
     * @date    2020/8/11
     **/
    private boolean createPathNode(String fullPath, Object data, CreateMode mode, StringBuilder path) {
        try {
            if (client.exists(path.toString())) {
                return false;
            }
            if (fullPath.equals(path.toString())) {
                client.create(path.toString(), data, mode);
            } else {
                client.create(path.toString(), null, mode);
            }
            return false;
        } catch (Exception e) {
            return true;
        }
    }

    /**
     * 注册任务执行器
     * @author xiaoqianbin
     * @date 2020/7/11
     **/
    public void registerExecutor() {
        try {
            executorName = InetAddress.getLocalHost().getHostName();
            replaceNode("/executors/" + executorName, null, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }

    /**
     * 替换已经存在的node
     * @param relative
     * @param data
     * @param mode
     * @author xiaoqianbin
     * @date 2020/7/11
     **/
    public String replaceNode(String relative, Object data, CreateMode mode) {
        removeNode(relative);
        createNode(namespace + relative, data, mode);
        return namespace + relative;
    }

    /**
     * 初始化目录节点
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    public void init() {
        client = new ZkClient(hosts);
        createNamespace();
        createNode("/executors");
        // dag
        createNode(GRAPHS);
        createNode("/meta");
        createNode(META_SCHEDULER);
        createNode(META_TASKS);
        registerExecutor();
    }

    /**
     * 订阅
     * @param relativePath
     * @param listener
     * @author xiaoqianbin
     * @date 2020/7/16
     **/
    public void subscribeChildChanges(String relativePath, IZkChildListener listener) {
        client.subscribeChildChanges(namespace + relativePath, listener);
    }

    /**
     * 订阅数据变化
     * @param	relativePath
	 * @param	listener
     * @author  xiaoqianbin
     * @date    2020/7/24
     **/
    public void subscribeDataChanges(String relativePath, IZkDataListener listener) {
        client.subscribeDataChanges(namespace + relativePath, listener);
    }

    /**
     * 订阅状态变更监听器
     * @param	listener
     * @author  xiaoqianbin
     * @date    2020/7/16
     **/
    public void subscribeStateChanges(IZkStateListener listener) {
        client.subscribeStateChanges(listener);
    }

    /**
     * 取消订阅
     * @param relativePath
     * @param listener
     * @author xiaoqianbin
     * @date 2020/7/16
     **/
    public void unsubscribeChildChanges(String relativePath, IZkChildListener listener) {
        client.unsubscribeChildChanges(namespace + relativePath, listener);
    }

    /**
     * 销毁客户端
     * @author xiaoqianbin
     * @date 2020/7/11
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
     * 判断相对节点是否存在
     * @param relativePath
     * @author xiaoqianbin
     * @date 2020/7/16
     **/
    public boolean exists(String relativePath) {
        return client.exists(namespace + relativePath);
    }

    /**
     * 注册任务节点
     * @param name
     * @param data
     * @param system
     * @author xiaoqianbin
     * @date 2020/7/13
     **/
    public void registerTaskMeta(String name, Object data, boolean system) {
        String metaPath = META_SCHEDULER;
        if (!system) {
            metaPath = META_TASKS;
        }
        String path = namespace + metaPath + "/" + name;
        if (client.exists(path)) {
            client.writeData(path, data);
        } else {
            createNode(path, data, CreateMode.PERSISTENT);
        }
    }

    /**
     * 直接创建节点
     * @param relativePath
     * @param data
     * @param mode
     * @author xiaoqianbin
     * @date 2020/7/16
     **/
    public String create(String relativePath, Object data, CreateMode mode) {
        return client.create(namespace + relativePath, data, mode);
    }

    /**
     * 读数据
     * @param relativePath
     * @author xiaoqianbin
     * @date 2020/7/16
     **/
    public <T> T readData(String relativePath) {
        return client.readData(namespace + relativePath);
    }

    /**
     * 写数据
     * @param	relativePath
	 * @param	data
     * @author  xiaoqianbin
     * @date    2020/7/24
     **/
    public void writeData(String relativePath, Object data) {
        client.writeData(namespace + relativePath, data);
    }

    /**
     * 查询子节点
     * @param relativePath
     * @author xiaoqianbin
     * @date 2020/7/16
     **/
    public List<String> getChildren(String relativePath) {
        try {
            return client.getChildren(namespace + relativePath);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return new ArrayList<>();
        }
    }


    /**
     * 删除节点
     * @param relative 相对于根节点的节点路径
     * @author xiaoqianbin
     * @date 2020/7/11
     **/
    private void removeNode(String relative) {
        if (client.exists(namespace + relative)) {
            client.delete(namespace + relative);
        }
    }

    /**
     * 删除path以及path下所有子节点
     * @param relativePath
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    public boolean deleteRecursive(String relativePath) {
        return client.deleteRecursive(namespace + relativePath);
    }

    /**
     * 删除相对路径
     * @param relative
     * @author xiaoqianbin
     * @date 2020/7/16
     **/
    public void delete(String relative) {
        client.delete(namespace + relative);
    }

    /**
     * 取消所有订阅
     * @author  xiaoqianbin
     * @date    2020/7/16
     **/
    public void unsubscribeAll() {
        client.unsubscribeAll();
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    public ZkClient getClient() {
        return client;
    }
}
