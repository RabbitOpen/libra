package rabbit.open.libra.client;

import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.CollectionUtils;
import rabbit.open.libra.client.exception.LibraException;
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
     * 任务缓存, 一级key是appName，二级key是group name，
     **/
    private static Map<String, Map<String, List<TaskMeta>>> taskMetaCache = new ConcurrentHashMap<>();

    /**
     * 任务运行状态
     **/
    private static boolean executeStatus = false;

    /**
     * 是否是系统任务
     * @author xiaoqianbin
     * @date 2020/7/13
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
        registerTaskMeta();
    }

    /**
     * 注册任务源信息
     * @author xiaoqianbin
     * @date 2020/7/16
     **/
    protected void registerTaskMeta() {
        getRegistryHelper().registerTaskMeta(getAppName() + PS + getTaskName(), new TaskMeta(this), isSystemTask());
    }

    /**
     * 完成任务信息注册工作
     * @param task
     * @author xiaoqianbin
     * @date 2020/7/11
     **/
    private synchronized static void register(AbstractLibraTask task) {
        if (!taskMetaCache.containsKey(task.getAppName())) {
            taskMetaCache.put(task.getAppName(), new ConcurrentHashMap<>(64));
        }
        Map<String, List<TaskMeta>> appMap = taskMetaCache.get(task.getAppName());
        if (!appMap.containsKey(task.getTaskGroup())) {
            appMap.put(task.getTaskGroup(), new ArrayList<>());
        }
        appMap.get(task.getTaskGroup()).add(new TaskMeta(task));
        appMap.get(task.getTaskGroup()).sort(Comparator.comparingInt(TaskMeta::getExecuteOrder).thenComparing(TaskMeta::getTaskName));
    }

    /**
     * 获取任务map
     * @author xiaoqianbin
     * @date 2020/7/11
     **/
    public static Map<String, List<TaskMeta>> getTaskMetaCache(String appName) {
        return taskMetaCache.get(appName);
    }

    /**
     * 启动分布式任务
     * @author xiaoqianbin
     * @date 2020/7/11
     **/
    public synchronized static void runScheduleTasks() {
        if (executeStatus) {
            return;
        }
        executeStatus = true;
        refreshMeta();
        Map<String, List<TaskMeta>> appMap = getTaskMetaCache(DEFAULT_APP);
        if (appMap.isEmpty()) {
            throw new LibraException("default-app is not existed!");
        }
        List<TaskMeta> scheduleTasks = appMap.get(SchedulerTask.SCHEDULE_GROUP);
        if (!CollectionUtils.isEmpty(scheduleTasks)) {
            scheduleTasks.forEach(t -> {
                t.getTaskPiece().execute(0, 1, null);
            });
        }
    }

    /**
     * 刷新meta信息, 移除多余的meta节点信息
     * @author xiaoqianbin
     * @date 2020/7/15
     **/
    private static void refreshMeta() {
        if (taskMetaCache.isEmpty()) {
            return;
        }
        Map<String, List<TaskMeta>> map = taskMetaCache.values().iterator().next();
        if (map.isEmpty()) {
            return;
        }
        AbstractLibraTask task = (AbstractLibraTask) map.values().iterator().next().get(0).getTaskPiece();
        RegistryHelper helper = task.getRegistryHelper();
        for (String app : taskMetaCache.keySet()) {
            refreshMetaByApp(helper, app);
        }
    }

    /**
     * 按app 刷新meta信息
     * @param    helper
     * @param    app
     * @author xiaoqianbin
     * @date 2020/7/16
     **/
    private static void refreshMetaByApp(RegistryHelper helper, String app) {
        List<String> tasks = helper.getChildren(RegistryHelper.TASKS_META_USERS + PS + app);
        for (String child : tasks) {
            boolean exists = false;
            Map<String, List<TaskMeta>> map = taskMetaCache.get(app);
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
                helper.delete(RegistryHelper.TASKS_META_USERS + PS + app + PS + child);
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
        closeScheduleTasks();

        // 关闭普通任务
        closeUserTasks();
    }

    /**
     * 关闭普通任务
     * @author  xiaoqianbin
     * @date    2020/7/16
     **/
    private static void closeUserTasks() {
        for (Map<String, List<TaskMeta>> map : taskMetaCache.values()) {
            for (Map.Entry<String, List<TaskMeta>> entry : map.entrySet()) {
                if (SchedulerTask.SCHEDULE_GROUP.equals(entry.getKey())) {
                    continue;
                }
                List<TaskMeta> tasks = entry.getValue();
                if (CollectionUtils.isEmpty(tasks)) {
                    continue;
                }
                for (TaskMeta task : tasks) {
                    closeTask(task);
                }
            }
        }
    }

    /**
     * 关闭调度任务
     * @author  xiaoqianbin
     * @date    2020/7/16
     **/
    private static void closeScheduleTasks() {
        List<TaskMeta> scheduleTasks = getTaskMetaCache(DEFAULT_APP).get(SchedulerTask.SCHEDULE_GROUP);
        if (!CollectionUtils.isEmpty(scheduleTasks)) {
            scheduleTasks.forEach(t -> {
                t.getTaskPiece().close();
            });
        }
    }

    /**
     * 安全退出任务
     * @param    task
     * @author xiaoqianbin
     * @date 2020/7/13
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
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    protected String getLeaderName() {
        return getHostName();
    }

    /**
     * 获取当前主机名
     * @author xiaoqianbin
     * @date 2020/7/14
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
