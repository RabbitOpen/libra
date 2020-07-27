package rabbit.open.libra.client;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import rabbit.open.libra.client.exception.LibraException;

import javax.annotation.PostConstruct;
import java.net.InetAddress;
import java.util.List;

/**
 * zk client 辅助类
 * @author xiaoqianbin
 * @date 2020/7/10
 **/
public class RegistryHelper {

    private Logger logger = LoggerFactory.getLogger(getClass());

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
     * 任务触发节点
     **/
    public static final String TASKS_EXECUTION_TRIGGER = "/tasks/execution/trigger";
    
    /**
     * 运行通知节点
     */
    public static final String TASKS_EXECUTION_NOTIFY = "/tasks/execution/notify";

    /**
     * 用户任务meta信息路径
     **/
    public static final String TASKS_META_USERS = "/tasks/meta/users";

    /**
     * 调度任务meta信息路径
     **/
    public static final String TASKS_META_SCHEDULE = "/tasks/meta/schedule";


    // zk地址
    @Value("${zookeeper.hosts.url:localhost:2181}")
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
     * @author xiaoqianbin
     * @date 2020/7/13
     **/
    private void createRootPath() {
        createPersistNode(rootPath, null, CreateMode.PERSISTENT, true);
    }

    /**
     * 递归创建永久节点
     * @param relativePath
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    public void createPersistNode(String relativePath) {
        createPersistNode(relativePath, false);
    }

    /**
     * 递归创建永久节点
     * @param	relativePath
	 * @param	ignoreError     忽略错误
     * @author  xiaoqianbin
     * @date    2020/7/16
     **/
    public void createPersistNode(String relativePath, boolean ignoreError) {
        createPersistNode(rootPath + relativePath, null, CreateMode.PERSISTENT, ignoreError);
    }

    /**
     * 递归创建永久节点
     * @param	relativePath
     * @param	data            数据
     * @param	ignoreError     忽略错误
     * @author  xiaoqianbin
     * @date    2020/7/16
     **/
    public void createPersistNode(String relativePath, Object data, boolean ignoreError) {
        createPersistNode(rootPath + relativePath, data, CreateMode.PERSISTENT, ignoreError);
    }

    /**
     * 递归创建节点
     * @param fullPath
     * @param data
     * @param mode
     * @param ignoreError
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    private void createPersistNode(String fullPath, Object data, CreateMode mode, boolean ignoreError) {
        String[] nodes = fullPath.split(AbstractLibraTask.PS);
        StringBuilder path = new StringBuilder();
        for (String node : nodes) {
            if (!"".equals(node.trim())) {
                path.append(AbstractLibraTask.PS).append(node);
            }
            try {
                if ("".equals(node.trim()) || client.exists(path.toString())) {
                    continue;
                }
                if (fullPath.equals(path.toString())) {
                    client.create(path.toString(), data, mode);
                } else {
                    client.create(path.toString(), null, mode);
                }
            } catch (Exception e) {
                if (!ignoreError) {
                    throw new LibraException(String.format("createPersistNode[%s] error, %s", fullPath, e.getMessage()));
                }
            }
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
        createPersistNode(rootPath + relative, data, mode, true);
        return rootPath + relative;
    }


    /**
     * 初始化目录节点
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    @PostConstruct
    public void init() {
        client = new ZkClient(hosts);
        createRootPath();
        createPersistNode("/executors");
        createPersistNode("/tasks");
        createPersistNode("/tasks/meta");
        createPersistNode(TASKS_META_SCHEDULE);
        createPersistNode(TASKS_META_USERS);
        createPersistNode("/tasks/execution");
        createPersistNode(TASKS_EXECUTION_USERS);
        createPersistNode(TASKS_EXECUTION_SCHEDULE);
        createPersistNode(TASKS_EXECUTION_TRIGGER);
        createPersistNode(TASKS_EXECUTION_NOTIFY);
        // 处于执行中的任务
        createPersistNode(TASKS_EXECUTION_RUNNING);
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
        client.subscribeChildChanges(rootPath + relativePath, listener);
    }

    /**
     * 订阅数据变化
     * @param	relativePath
	 * @param	listener
     * @author  xiaoqianbin
     * @date    2020/7/24
     **/
    public void subscribeDataChanges(String relativePath, IZkDataListener listener) {
        client.subscribeDataChanges(rootPath + relativePath, listener);
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
        client.unsubscribeChildChanges(rootPath + relativePath, listener);
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
        return client.exists(rootPath + relativePath);
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
        String metaPath = TASKS_META_SCHEDULE;
        if (!system) {
            metaPath = TASKS_META_USERS;
        }
        replaceNode(metaPath + "/" + name, data, CreateMode.PERSISTENT);
    }

    /**
     * 直接创建节点
     * @param path
     * @param data
     * @param mode
     * @author xiaoqianbin
     * @date 2020/7/16
     **/
    public String create(String path, Object data, CreateMode mode) {
        return client.create(rootPath + path, data, mode);
    }

    /**
     * 读数据
     * @param relativePath
     * @author xiaoqianbin
     * @date 2020/7/16
     **/
    public <T> T readData(String relativePath) {
        return client.readData(rootPath + relativePath);
    }

    /**
     * 写数据
     * @param	relativePath
	 * @param	data
     * @author  xiaoqianbin
     * @date    2020/7/24
     **/
    public void writeData(String relativePath, Object data) {
        client.writeData(rootPath + relativePath, data);
    }

    /**
     * 查询子节点
     * @param relativePath
     * @author xiaoqianbin
     * @date 2020/7/16
     **/
    public List<String> getChildren(String relativePath) {
        return client.getChildren(rootPath + relativePath);
    }

    /**
     * 发布任务
     * @param	appName
     * @param	groupName
     * @param	taskName
     * @param	scheduleTime
     * @param	group           true: 发布整组任务
     * @author  xiaoqianbin
     * @date    2020/7/24
     **/
    public void publishTask(String appName, String groupName, String taskName, String scheduleTime, boolean group) {
        String desc = appName + "@" + groupName + "@" + taskName + "@" + scheduleTime + "@" +
                (group ? ManualScheduleType.GROUP : ManualScheduleType.SINGLE);
        createPersistNode(RegistryHelper.TASKS_EXECUTION_TRIGGER + "/" + desc);
        writeData(RegistryHelper.TASKS_EXECUTION_TRIGGER, "go");
    }

    /**
     * 删除节点
     * @param relative 相对于根节点的节点路径
     * @author xiaoqianbin
     * @date 2020/7/11
     **/
    private void removeNode(String relative) {
        if (client.exists(rootPath + relative)) {
            client.delete(rootPath + relative);
        }
    }

    /**
     * 删除path以及path下所有子节点
     * @param relativePath
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    public void deleteNode(String relativePath) {
        client.deleteRecursive(rootPath + relativePath);
    }

    /**
     * 删除相对路径
     * @param relative
     * @author xiaoqianbin
     * @date 2020/7/16
     **/
    public void delete(String relative) {
        client.delete(rootPath + relative);
    }

    /**
     * 取消所有订阅
     * @author  xiaoqianbin
     * @date    2020/7/16
     **/
    public void unsubscribeAll() {
        client.unsubscribeAll();
    }

    public void setRootPath(String rootPath) {
        this.rootPath = rootPath;
    }

    public void setHosts(String hosts) {
        this.hosts = hosts;
    }
    
}
