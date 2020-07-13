package rabbit.open.libra.client;

import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.CollectionUtils;
import rabbit.open.libra.client.task.SchedulerTask;

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
        getRegistryHelper().registerTaskMeta(getTaskName(), new TaskMeta(this), isSystemTask());
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
        taskMap.get(task.getTaskGroup()).sort(Comparator.comparing(t -> t.getTaskPiece().getExecuteOrder()));
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
        List<TaskMeta> systemTasks = getTaskMap().get(SchedulerTask.GROUP_NAME);
        if (!CollectionUtils.isEmpty(systemTasks)) {
            systemTasks.forEach(t -> {
                t.getTaskPiece().execute(0, 1);
            });
        }
    }

    /**
     * 退出分布式任务
     * @author xiaoqianbin
     * @date 2020/7/11
     **/
    public synchronized static void shutdown() {
        List<TaskMeta> systemTasks = getTaskMap().get(SchedulerTask.GROUP_NAME);
        if (!CollectionUtils.isEmpty(systemTasks)) {
            systemTasks.forEach(t -> {
                t.getTaskPiece().close();
            });
        }
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
        if (SchedulerTask.GROUP_NAME.equals(task.getTaskPiece().getTaskGroup())) {
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
        return try2AcquireControl(path, null, mode);
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
            getRegistryHelper().getClient().create(path, data, mode);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
