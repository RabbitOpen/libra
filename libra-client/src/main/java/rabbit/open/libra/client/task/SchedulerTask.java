package rabbit.open.libra.client.task;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import rabbit.open.libra.client.AbstractLibraTask;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.TaskMeta;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * 调度任务，分布式任务的调度核心，负责调度所有其他任务
 * @author xiaoqianbin
 * @date 2020/7/11
 **/
public class SchedulerTask extends AbstractLibraTask {

    private Logger logger = LoggerFactory.getLogger(getClass());

    public final static String GROUP_NAME = "SYSTEM";

    private Thread schedulerThread;

    private boolean closed = false;

    @Autowired
    private RegistryHelper helper;

    /**
     * 任务元信息
     * @author  xiaoqianbin
     * @date    2020/7/14
     **/
    private Map<String, List<TaskMeta>> taskMetaMap = new ConcurrentHashMap<>();

    // 任务调度map，key是task group， value是task name
    private Map<String, String> taskScheduleMap = new ConcurrentHashMap<>();

    /**
     * 监控线程阻塞信号
     * @author xiaoqianbin
     * @date 2020/7/13
     **/
    private Semaphore blockingSemaphore = new Semaphore(0);

    @Override
    public RegistryHelper getRegistryHelper() {
        return helper;
    }

    /**
     * 执行任务
     * @param	index
	 * @param	splits
	 * @param	taskScheduleTime
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    @Override
    public void execute(int index, int splits, String taskScheduleTime) {
        ZkClient zkClient = getRegistryHelper().getClient();
        String sysNode = getRegistryHelper().getRootPath() + "/tasks/execution/system";
        String sysPath = sysNode + "/" + getTaskName();
        zkClient.subscribeChildChanges(sysNode, (path, list) -> {
            logger.info("path 【{}】 children changed, {}", path, list);
            if (!list.contains(getTaskName()) && try2AcquireControl(sysPath, CreateMode.EPHEMERAL)) {
                logger.info("running SchedulerTask in [active] mode");
                blockingSemaphore.release();
            }
        });
        if (try2AcquireControl(sysPath, CreateMode.EPHEMERAL)) {
            logger.info("running SchedulerTask in [active] mode");
            blockingSemaphore.release();
        } else {
            logger.info("running SchedulerTask in [standby] mode");
        }
        schedulerThread = new Thread(() -> {
            while (true) {
                try {
                    blockingSemaphore.acquire();
                    if (closed) {
                        break;
                    }
                    doSchedule();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
            logger.info("scheduler thread is exited....");
        });
        schedulerThread.setDaemon(false);
        schedulerThread.start();
    }

    /**
     * 执行调度
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    protected void doSchedule() {
        // 加载任务元信息
        loadTaskMetas();
        doRecoveryChecking();
    }

    /**
     * 加载任务元信息
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    protected void loadTaskMetas() {
        RegistryHelper helper = getRegistryHelper();
        String taskPath = helper.getRootPath() + "/tasks/meta/users";
        List<String> children = helper.getClient().getChildren(taskPath);
        taskMetaMap.clear();
        for (String child : children) {
            TaskMeta meta = helper.getClient().readData(taskPath + "/" + child);
            if (!taskMetaMap.containsKey(meta.getGroupName())) {
                taskMetaMap.put(meta.getGroupName(), new ArrayList<>());
            }
            taskMetaMap.get(meta.getGroupName()).add(meta);
            // 排序
            taskMetaMap.get(meta.getGroupName()).sort(Comparator.comparing(TaskMeta::getExecuteOrder)
                    .thenComparing(TaskMeta::getTaskName));
        }
        // print logs
        for (Map.Entry<String, List<TaskMeta>> entry : taskMetaMap.entrySet()) {
            logger.info("found task, group: [{}], tasks: {}", entry.getKey(), entry.getValue());
        }
    }

    /**
     * 任务恢复检查
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    protected void doRecoveryChecking() {
        RegistryHelper helper = getRegistryHelper();
        String scheduleTaskPath = helper.getRootPath() + "/tasks/execution/schedule";
        List<String> scheduleGroups = helper.getClient().getChildren(scheduleTaskPath);
        for (String group : scheduleGroups) {
            List<String> scheduleTasks = getRegistryHelper().getClient().getChildren(scheduleTaskPath + "/" + group);
            if (scheduleTasks.isEmpty()) {
                continue;
            }
            scheduleTasks.sort(String::compareTo);
            taskScheduleMap.put(group, scheduleTasks.get(scheduleTasks.size() - 1));
        }
    }

    @Override
    protected final String getTaskGroup() {
        return GROUP_NAME;
    }

    @Override
    protected final boolean isSystemTask() {
        return true;
    }

    @Override
    protected void close() {
        logger.info("scheduler is closing......");
        closed = true;
        blockingSemaphore.release();
    }
}
