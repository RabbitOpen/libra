package rabbit.open.libra.client.task;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
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
import java.util.stream.Collectors;

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
     * 任务元信息 key: group名
     * @author  xiaoqianbin
     * @date    2020/7/14
     **/
    private Map<String, List<TaskMeta>> taskMetaMap = new ConcurrentHashMap<>();

    /**
     * 节点监听器列表
     * @author  xiaoqianbin
     * @date    2020/7/14
     **/
    private Map<String, IZkChildListener> listenerMap = new ConcurrentHashMap<>();

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
        // 注册网络事件监听
        registerStateChangeListener(zkClient);
        // 抢占控制权
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
        startScheduleThread();
    }

    /**
     * 启动调度线程
     * @author  xiaoqianbin
     * @date    2020/7/14
     **/
    private void startScheduleThread() {
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
     * 注册网络事件
     * @param	zkClient
     * @author  xiaoqianbin
     * @date    2020/7/14
     **/
    private void registerStateChangeListener(ZkClient zkClient) {

        zkClient.subscribeStateChanges(new IZkStateListener() {

            @Override
            public void handleStateChanged(Watcher.Event.KeeperState keeperState) throws Exception {
                if (Watcher.Event.KeeperState.Disconnected == keeperState) {
                    logger.error("network error is inspected");
                }
                if (Watcher.Event.KeeperState.SyncConnected == keeperState) {
                    logger.info("network error is recovered");
                }
            }

            @Override
            public void handleNewSession() throws Exception {

            }

            @Override
            public void handleSessionEstablishmentError(Throwable throwable) throws Exception {

            }
        });
    }

    /**
     * 执行调度
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    protected void doSchedule() {
        // 加载任务元信息
        loadTaskMetas();
        // 尝试恢复未完成的任务
        recoverUnFinishedTasks();

        // todo 定时调度未被调度的节点

        // TODO 订阅事件（重连）

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
     * 任务恢复
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    protected void recoverUnFinishedTasks() {
        RegistryHelper helper = getRegistryHelper();
        String scheduleTaskPath = helper.getRootPath() + "/tasks/execution/schedule";
        List<String> scheduleGroups = helper.getClient().getChildren(scheduleTaskPath);
        for (String group : scheduleGroups) {
            List<String> scheduleTasks = getRegistryHelper().getClient().getChildren(scheduleTaskPath + "/" + group);
            if (scheduleTasks.isEmpty()) {
                continue;
            }
            scheduleTasks.sort(String::compareTo);
            // 移除 "/libra/root/tasks/execution/schedule/{groupName}/{taskName}" 下的脏数据
            removeDirtyRegistryInformation(scheduleTaskPath, group, scheduleTasks);

            // 注册 "/libra/root/tasks/execution/users/{taskName}/{scheduleTime}" 的监听器
            registerTaskExecutionListener(scheduleTaskPath, group, scheduleTasks);
        }
    }

    /**
     * 注册任务执行情况监听器
     * @param	scheduleTaskPath
	 * @param	group
	 * @param	scheduleTasks
     * @author  xiaoqianbin
     * @date    2020/7/14
     **/
    private void registerTaskExecutionListener(String scheduleTaskPath, String group, List<String> scheduleTasks) {
        String taskName = scheduleTasks.get(scheduleTasks.size() - 1);
        String path = scheduleTaskPath + "/" + group + "/" + taskName;
        // 一个任务可能有多个调度在执行
        List<String> scheduleTimes = getRegistryHelper().getClient().getChildren(path);
        for (String scheduleTime : scheduleTimes) {
            String execPath = getRegistryHelper().getRootPath() + "/tasks/execution/users/" + taskName + "/" + scheduleTime;
            if (getRegistryHelper().getClient().exists(execPath)) {
                // schedule节点中有数据，运行节点没数据
                getRegistryHelper().createPersistNode(execPath);
            }
            IZkChildListener listener = createExecutionListener(group, taskName, scheduleTime);
            listenerMap.put(execPath, listener);
            getRegistryHelper().getClient().subscribeChildChanges(execPath, listener);
            // 检测下任务的完成状态，如果完成了，需要更新下执行节点
            checkExecutionStatus(group, taskName, scheduleTime);
        }
    }

    /**
     * 创建一个节点监听器
     * @param	group
	 * @param	taskName
	 * @param	scheduleTime
     * @author  xiaoqianbin
     * @date    2020/7/14
     **/
    private IZkChildListener createExecutionListener(String group, String taskName, String scheduleTime) {
        return (nodePath, list) -> {
            checkExecutionStatus(group, taskName, scheduleTime);
        };
    }

    /**
     * 检测下任务的完成状态，如果完成了，需要更新下执行节点
     * @param	group
	 * @param	taskName
	 * @param	scheduleTime
     * @author  xiaoqianbin
     * @date    2020/7/14
     **/
    private void checkExecutionStatus(String group, String taskName, String scheduleTime) {
        String execPath = getRegistryHelper().getRootPath() + "/tasks/execution/users/" + taskName + "/" + scheduleTime;
        synchronized (listenerMap.get(execPath)) {
            List<String> children = getRegistryHelper().getClient().getChildren(execPath);
            Map<Boolean, List<String>> statusMap = children.stream().collect(Collectors.groupingBy(s -> s.startsWith(RUNNING_TASK_PREFIX)));
            int splitsCount = taskMetaMap.get(group).stream().filter(t -> t.getTaskName().equals(taskName)).collect(Collectors.toList()).get(0).getSplitsCount();
            if (statusMap.get(false).size() != splitsCount) {
                // 还有未完成的分片 直接跳过
                return;
            }
            getRegistryHelper().getClient().unsubscribeChildChanges(execPath, listenerMap.get(execPath));
            String nextTask = getNextTask(group, taskName);
            String scheduleRoot = getRegistryHelper().getRootPath() + "/tasks/execution/schedule/" + group;
            if (null != nextTask) {
                // 调度分组中的下一个任务
                getRegistryHelper().createPersistNode(scheduleRoot + "/" + nextTask + "/" + scheduleTime);
                removeLastTaskScheduleInfo(taskName, scheduleTime, scheduleRoot);
                String nextExecPath = getRegistryHelper().getRootPath() + "/tasks/execution/users/" + nextTask + "/" + scheduleTime;
                getRegistryHelper().createPersistNode(nextExecPath);
                // 注册下个节点的监听事件
                IZkChildListener listener = createExecutionListener(group, nextTask, scheduleTime);
                listenerMap.put(execPath, listener);
                getRegistryHelper().getClient().subscribeChildChanges(execPath, listener);
            } else {
                removeLastTaskScheduleInfo(taskName, scheduleTime, scheduleRoot);
            }
        }
    }

    /**
     * 移除已完成的节点schedule信息
     * @param	taskName        任务名
	 * @param	scheduleTime    scheduleTime
	 * @param	scheduleRoot    {rootPath} + "/tasks/execution/schedule/" + group
     * @author  xiaoqianbin
     * @date    2020/7/14
     **/
    private void removeLastTaskScheduleInfo(String taskName, String scheduleTime, String scheduleRoot) {
        getRegistryHelper().deleteNode(scheduleRoot + "/" + taskName + "/" + scheduleTime);
        if (getRegistryHelper().getClient().getChildren(scheduleRoot + taskName).isEmpty()) {
            getRegistryHelper().deleteNode(scheduleRoot + "/" + taskName);
        }
    }

    /**
     * 获取分组中的下一个任务
     * @param	group
	 * @param	taskName
     * @author  xiaoqianbin
     * @date    2020/7/14
     **/
    private String getNextTask(String group, String taskName) {
        List<TaskMeta> groupTasks = getTaskMap().get(group);
        for (int i = 0; i < groupTasks.size(); i++) {
            if (taskName.equals(groupTasks.get(i).getTaskName()) && i != (groupTasks.size() - 1)) {
                return groupTasks.get(i + 1).getTaskName();
            }
        }
        return null;
    }

    /**
     * 移除多余的注册信息（意外终止可能来不及清理的执行信息）
     * @param	scheduleTaskPath
	 * @param	group
	 * @param	scheduleTasks
     * @author  xiaoqianbin
     * @date    2020/7/14
     **/
    private void removeDirtyRegistryInformation(String scheduleTaskPath, String group, List<String> scheduleTasks) {
        if (scheduleTasks.size() > 1) {
            for (int i = 0; i < scheduleTasks.size() - 1; i++) {
                getRegistryHelper().getClient().delete(scheduleTaskPath + "/" + group + "/" + scheduleTasks.get(i));
            }
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
        helper.getClient().unsubscribeAll();
        blockingSemaphore.release();
    }

    @Override
    protected List<String> getCrones() {
        return new ArrayList<>();
    }
}
