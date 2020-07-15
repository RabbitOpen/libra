package rabbit.open.libra.client;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetAddress;
import java.util.List;

/**
 * zk client 辅助类
 * @author xiaoqianbin
 * @date 2020/7/10
 **/
public class RegistryHelper {

    /**
     * 运行信息路径
     **/
    public static final String TASKS_EXECUTION_RUNNING = "/tasks/execution/running";

    /**
     * 调度任务节点路径
     **/
    public static final String TASKS_EXECUTION_SCHEDULE = "/tasks/execution/schedule";

    /**
     * 用户任务节点路径
     **/
    public static final String TASKS_EXECUTION_USERS = "/tasks/execution/users";

    /**
     * 用户任务meta信息路径
     **/
    public static final String TASKS_META_USERS = "/tasks/meta/users";

    /**
     * 调度任务meta信息路径
     **/
    public static final String TASKS_META_SCHEDULE = "/tasks/meta/schedule";

    private Logger logger = LoggerFactory.getLogger(getClass());

    // zk地址
    @Value("${zookeeper.hosts.url}")
    private String hosts = "localhost:2181";

    // 监控根节点
    @Value("${libra.monitor.root-path}")
    private String rootPath = "/libra/root";

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
        createPersistNode(rootPath);
    }

    /**
     * 递归创建永久节点
     * @param	fullPath
     * @author  xiaoqianbin
     * @date    2020/7/14
     **/
    public void createPersistNode(String fullPath) {
        createPersistNode(fullPath, null);
    }

    /**
     * 递归创建永久节点
     * @param	fullPath
     * @param	data
     * @author  xiaoqianbin
     * @date    2020/7/14
     **/
    public void createPersistNode(String fullPath, Object data) {
        String[] nodes = fullPath.split("/");
        String path = "";
        for (String node : nodes) {
            if ("".equals(node.trim())) {
                continue;
            }
            path = path + "/" + node;
            if (client.exists(path)) {
                continue;
            }
            try {
                client.create(path, data, CreateMode.PERSISTENT);
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
    public void registerExecutor() {
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
            return createPersistNode(nodePath, null, CreateMode.PERSISTENT);
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
        return createPersistNode(relative, data, mode);
    }

    /**
     * 创建节点
     * @param	relative
	 * @param	data
	 * @param	mode
     * @author  xiaoqianbin
     * @date    2020/7/11
     **/
    public String createPersistNode(String relative, Object data, CreateMode mode) {
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
        createPersistentNode(TASKS_META_SCHEDULE);
        createPersistentNode(TASKS_META_USERS);
        createPersistentNode("/tasks/execution");
        createPersistentNode(TASKS_EXECUTION_USERS);
        createPersistentNode(TASKS_EXECUTION_SCHEDULE);
        // 处于执行中的任务
        createPersistentNode(TASKS_EXECUTION_RUNNING);
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
        String metaPath = TASKS_META_SCHEDULE;
        if (!system) {
            metaPath = TASKS_META_USERS;
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

    /**
     * 删除path以及path下所有子节点
     * @param	path
     * @author  xiaoqianbin
     * @date    2020/7/14
     **/
    public void deleteNode(String path) {
        if (client.exists(path)) {
            List<String> children = client.getChildren(path);
            if (!children.isEmpty()) {
                for (String child : children) {
                    deleteNode(path + "/" + child);
                }
            }
            client.delete(path);
        }
    }
}
