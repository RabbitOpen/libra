package rabbit.open.libra.client;

import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.CollectionUtils;
import rabbit.open.libra.client.task.SchedulerTask;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 抽象分布式任务
 * @author xiaoqianbin
 * @date 2020/7/11
 **/
public abstract class AbstractLibraTask extends TaskPiece implements InitializingBean {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 正在运行的任务的前缀
     **/
    public final static String RUNNING_TASK_PREFIX = "R-";

    /**
     * path separator
     **/
    public static final String PS = "/";

    /**
     * 任务缓存
     * @author xiaoqianbin
     * @date 2020/7/11
     **/
    private static Map<String, List<TaskMeta>> taskMap = new ConcurrentHashMap<>();

    // 任务运行状态
    private static boolean executeStatus = false;

    /**
     * 是否是系统任务
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    protected boolean isSystemTask() {
        return false;
    }

    /**
     * 获取zk客户端工具
     * @author xiaoqianbin
     * @date 2020/7/11
     **/
    public abstract RegistryHelper getRegistryHelper();

    @Override
    public void afterPropertiesSet() throws Exception {
        register(this);
        getRegistryHelper().registerTaskMeta(getAppName() + PS + getTaskName(), new TaskMeta(this), isSystemTask());
    }

    /**
     * 完成任务信息注册工作
     * @param    task
     * @author xiaoqianbin
     * @date 2020/7/11
     **/
    private synchronized static void register(AbstractLibraTask task) {
        if (!taskMap.containsKey(task.getTaskGroup())) {
            taskMap.put(task.getTaskGroup(), new ArrayList<>());
        }
        taskMap.get(task.getTaskGroup()).add(new TaskMeta(task));
        taskMap.get(task.getTaskGroup()).sort(Comparator.comparingInt(TaskMeta::getExecuteOrder).thenComparing(TaskMeta::getTaskName));
    }

    /**
     * 获取任务map
     * @author xiaoqianbin
     * @date 2020/7/11
     **/
    public static Map<String, List<TaskMeta>> getTaskMap() {
        return taskMap;
    }

    /**
     * 启动分布式任务
     * @author xiaoqianbin
     * @date 2020/7/11
     **/
    public synchronized static void runSystemTasks() {
        if (executeStatus) {
            return;
        }
        executeStatus = true;
        refreshMeta();
        List<TaskMeta> systemTasks = getTaskMap().get(SchedulerTask.SCHEDULE_GROUP);
        if (!CollectionUtils.isEmpty(systemTasks)) {
            systemTasks.forEach(t -> {
                t.getTaskPiece().execute(0, 1, null);
            });
        }
    }

    /**
     * 刷新meta信息, 移除多余的meta节点信息
     * @author  xiaoqianbin
     * @date    2020/7/15
     **/
    private static void refreshMeta() {
        Map<String, List<TaskMeta>> map = getTaskMap();
        if (map.isEmpty()) {
            return;
        }
        AbstractLibraTask task = (AbstractLibraTask) map.values().iterator().next().get(0).getTaskPiece();
        RegistryHelper helper = task.getRegistryHelper();
        List<String> children = helper.getChildren(RegistryHelper.TASKS_META_USERS);
        for (String child : children) {
            boolean exists = false;
            for (List<TaskMeta> metas : map.values()) {
                for (TaskMeta meta : metas) {
                    if (meta.getTaskName().equals(child)) {
                        exists = true;
                        break;
                    }
                }
                if (exists) {
                    break;
                }
            }
            if (!exists) {
                helper.delete(RegistryHelper.TASKS_META_USERS + PS + child);
            }
        }
    }

    /**
     * 退出分布式任务
     * @author xiaoqianbin
     * @date 2020/7/11
     **/
    public synchronized static void shutdown() {
        // 关闭调度任务
        List<TaskMeta> systemTasks = getTaskMap().get(SchedulerTask.SCHEDULE_GROUP);
        if (!CollectionUtils.isEmpty(systemTasks)) {
            systemTasks.forEach(t -> {
                t.getTaskPiece().close();
            });
        }

        // 关闭普通任务
        Map<String, List<TaskMeta>> taskMap = getTaskMap();
        for (Map.Entry<String, List<TaskMeta>> entry : taskMap.entrySet()) {
            List<TaskMeta> tasks = entry.getValue();
            if (CollectionUtils.isEmpty(tasks)) {
                continue;
            }
            for (TaskMeta task : tasks) {
                closeTask(task);
            }
        }
    }

    /**
     * 安全退出任务
     * @param	task
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    private static void closeTask(TaskMeta task) {
        if (SchedulerTask.SCHEDULE_GROUP.equals(task.getTaskPiece().getTaskGroup())) {
            return;
        }
        task.getTaskPiece().close();
    }

    /**
     * 尝试获取控制权
     * @param path
     * @param mode
     * @author xiaoqianbin
     * @date 2020/7/13
     **/
    protected boolean try2AcquireControl(String path, CreateMode mode) {
        return try2AcquireControl(path, getLeaderName(), mode);
    }

    /**
     * 获取主节点名
     * @author  xiaoqianbin
     * @date    2020/7/14
     **/
    protected String getLeaderName() {
        return getHostName();
    }

    /**
     * 获取当前主机名
     * @author  xiaoqianbin
     * @date    2020/7/14
     **/
    protected String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "UnknownHost";
        }
    }

    /**
     * 尝试获取控制权
     * @param path
     * @param data
     * @param mode
     * @author xiaoqianbin
     * @date 2020/7/13
     **/
    protected boolean try2AcquireControl(String path, Object data, CreateMode mode) {
        try {
            getRegistryHelper().create(path, data, mode);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
