package rabbit.open.libra.client;

import org.springframework.beans.factory.InitializingBean;
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

    /**
     * 任务缓存
     * @author  xiaoqianbin
     * @date    2020/7/11
     **/
    private static Map<String, List<TaskMeta>> taskMap = new ConcurrentHashMap<>();

    // 任务运行状态
    private static boolean executeStatus = false;

    /**
     * 获取zk客户端工具
     * @author  xiaoqianbin
     * @date    2020/7/11
     **/
    public abstract RegistryHelper getRegistryHelper();

    @Override
    public final void afterPropertiesSet() throws Exception {
        register(this);
    }

    /**
     * 完成任务信息注册工作
     * @param	task
     * @author  xiaoqianbin
     * @date    2020/7/11
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
     * @author  xiaoqianbin
     * @date    2020/7/11
     **/
    public static Map<String, List<TaskMeta>> getTaskMap() {
        return taskMap;
    }

    /**
     * 启动分布式任务
     * @author  xiaoqianbin
     * @date    2020/7/11
     **/
    public synchronized static void runTasks() {
        if (executeStatus) {
            return;
        }
        executeStatus = true;
        getTaskMap().get(SchedulerTask.GROUP_NAME).forEach(t -> {
            t.getTaskPiece().execute(0, 1);
        });
    }

    /**
     * 退出分布式任务
     * @author  xiaoqianbin
     * @date    2020/7/11
     **/
    public synchronized static void shutdown() {

    }
}
