package rabbit.open.libra.client.task;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;
import rabbit.open.libra.client.AbstractLibraTask;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.TaskMeta;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 调度任务，分布式任务的调度核心，负责调度所有其他任务
 * @author xiaoqianbin
 * @date 2020/7/11
 **/
public class SchedulerTask extends AbstractLibraTask {

    private Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 调度组
     * @author xiaoqianbin
     * @date 2020/7/15
     **/
    public final static String SCHEDULE_GROUP = "SYSTEM";

    /**
     * 调度线程
     * @author xiaoqianbin
     * @date 2020/7/15
     **/
    private Thread schedulerThread;

    private boolean closed = false;

    /**
     * leader   节点
     * @author xiaoqianbin
     * @date 2020/7/15
     **/
    private boolean leader = false;

    @Autowired
    private RegistryHelper helper;

    /**
     * 任务元信息 key: group名
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    private Map<String, List<TaskMeta>> taskMetaMap = new ConcurrentHashMap<>();

    /**
     * 节点监听器列表
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    private Map<String, IZkChildListener> listenerMap = new ConcurrentHashMap<>();

    /**
     * 监控线程阻塞信号
     * @author xiaoqianbin
     * @date 2020/7/13
     **/
    private Semaphore intervalSemaphore = new Semaphore(0);

    /**
     * 调度信号
     **/
    private Semaphore scheduleSemaphore = new Semaphore(0);

    /**
     * 任务组调度时间
     * @author  xiaoqianbin
     * @date    2020/7/15
     **/
    private Map<String, Date> groupScheduleMap = new ConcurrentHashMap<>();

    @Override
    public RegistryHelper getRegistryHelper() {
        return helper;
    }

    /**
     * 执行任务
     * @param index
     * @param splits
     * @param taskScheduleTime
     * @author xiaoqianbin
     * @date 2020/7/13
     **/
    @Override
    public void execute(int index, int splits, String taskScheduleTime) {
        ZkClient zkClient = getRegistryHelper().getClient();
        String scheduleNode = getRegistryHelper().getRootPath() + RegistryHelper.TASKS_EXECUTION_SCHEDULE;
        String schedulePath = scheduleNode + PS + getTaskName();
        // 注册网络事件监听
        registerStateChangeListener(zkClient);
        // 抢占控制权
        zkClient.subscribeChildChanges(scheduleNode, (path, list) -> {
            if (!list.contains(getTaskName())) {
                logger.info("leader is lost");
                try2AcquireControl(schedulePath, CreateMode.EPHEMERAL);
            } else {
                if (getLeaderName().equals(getRegistryHelper().getClient().readData(schedulePath))) {
                    leader = true;
                } else {
                    leader = false;
                }
            }
        });
        try2AcquireControl(schedulePath, CreateMode.EPHEMERAL);
        startScheduleThread();
    }

    /**
     * 启动调度线程
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    private void startScheduleThread() {
        schedulerThread = new Thread(() -> {
            while (true) {
                try {
                    intervalSemaphore.tryAcquire(3, TimeUnit.SECONDS);
                    if (closed) {
                        break;
                    }
                    if (leader) {
                        beginSchedule();
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
            logger.info("scheduler thread is exited....");
        }, getTaskName());
        schedulerThread.setDaemon(false);
        schedulerThread.start();
    }

    /**
     * 注册网络事件
     * @param zkClient
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    private void registerStateChangeListener(ZkClient zkClient) {

        zkClient.subscribeStateChanges(new IZkStateListener() {

            /**
             * 表示服务是否丢失过
             * @date 2020/7/15
             **/
            private boolean lost = false;

            @Override
            public void handleStateChanged(Watcher.Event.KeeperState keeperState) throws Exception {
                if (Watcher.Event.KeeperState.Disconnected == keeperState) {
                    serverLost();
                }
                if (Watcher.Event.KeeperState.SyncConnected == keeperState) {
                    serverConnected();
                }
            }

            /**
             * server连接成功
             * @author xiaoqianbin
             * @date 2020/7/15
             **/
            private void serverConnected() {
                if (!lost) {
                    return;
                }
                // 丢失后重连
                logger.info("zookeeper server is found");
                getRegistryHelper().registerExecutor();
                ZkClient client = getRegistryHelper().getClient();
                String scheduleNode = getRegistryHelper().getRootPath() + RegistryHelper.TASKS_EXECUTION_SCHEDULE;
                String schedulePath = scheduleNode + PS + getTaskName();
                if (client.exists(schedulePath)) {
                    if (getLeaderName().equals(client.readData(schedulePath))) {
                        leader = true;
                    }
                } else {
                    try2AcquireControl(schedulePath, CreateMode.EPHEMERAL);
                }
            }

            /**
             * server节点丢失
             * @author xiaoqianbin
             * @date 2020/7/15
             **/
            private void serverLost() {
                logger.error("zookeeper server is lost");
                lost = true;
                leader = false;
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
     * @author xiaoqianbin
     * @date 2020/7/13
     **/
    protected void beginSchedule() {
        // 加载任务元信息
        loadTaskMetas();
        // 尝试恢复未完成的任务
        recoverUnFinishedTasks();
        // 定时调度未被调度的节点
        doSchedule();
    }

    /**
     * 定时调度的任务节点
     * @author xiaoqianbin
     * @date 2020/7/15
     **/
    private void doSchedule() {
        logger.info("scheduler job is running....");
        while (true) {
            try {
                if (closed || !leader) {
                    // 如果leader不是自己
                    break;
                }
                for (Map.Entry<String, List<TaskMeta>> entry : taskMetaMap.entrySet()) {
                    if (SCHEDULE_GROUP.equals(entry.getKey())) {
                        continue;
                    }
                    tryScheduleTaskGroup(entry.getValue());
                }
                scheduleSemaphore.tryAcquire(3, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.info(e.getMessage(), e);
            }
        }
        logger.info("scheduler job is stopped!");
    }

    /**
     * 尝试调度任务组
     * @param	groupMetas
     * @author  xiaoqianbin
     * @date    2020/7/15
     **/
    private void tryScheduleTaskGroup(List<TaskMeta> groupMetas) throws ParseException {
        if (groupMetas.isEmpty()) {
            return;
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String taskName = groupMetas.get(0).getTaskName();
        String group = groupMetas.get(0).getGroupName();
        Date nextScheduleTime = getNextScheduleTime(groupMetas);
        if (nextScheduleTime.before(new Date())) {
            RegistryHelper helper = getRegistryHelper();
            String schedule = sdf.format(nextScheduleTime);
            String executePath = helper.getRootPath() + RegistryHelper.TASKS_EXECUTION_USERS + PS + taskName + PS + schedule;
            helper.createPersistNode(executePath);
            groupScheduleMap.remove(group);
            logger.info("task group[{}] is scheduled at [{}]", group, schedule);
            helper.createPersistNode(helper.getRootPath() + RegistryHelper.TASKS_EXECUTION_RUNNING + PS + group + PS + schedule);
        }
    }

    /**
     * 获取任务组下次调度的时间
	 * @param	groupMetas
     * @author  xiaoqianbin
     * @date    2020/7/15
     **/
    private Date getNextScheduleTime(List<TaskMeta> groupMetas) throws ParseException {
        String group = groupMetas.get(0).getGroupName();
        String taskName = groupMetas.get(0).getTaskName();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        RegistryHelper helper = getRegistryHelper();
        if (groupScheduleMap.containsKey(group)) {
            String scheduleTask = helper.getRootPath() + RegistryHelper.TASKS_EXECUTION_USERS + PS + taskName + PS + sdf.format(groupScheduleMap.get(group));
            if (!helper.getClient().exists(scheduleTask)) {
                return groupScheduleMap.get(group);
            }
        }
        // 历史schedule
        List<String> historySchedules = helper.getClient().getChildren(helper.getRootPath() + RegistryHelper.TASKS_EXECUTION_USERS + PS + taskName);
        Date nextScheduleTime;
        if (CollectionUtils.isEmpty(historySchedules)) {
            nextScheduleTime = groupMetas.get(0).getNextScheduleTime(null);
        } else {
            historySchedules.sort(String::compareTo);
            nextScheduleTime = groupMetas.get(0).getNextScheduleTime(sdf.parse(historySchedules.get(historySchedules.size() - 1)));
        }
        groupScheduleMap.put(group, nextScheduleTime);
        return nextScheduleTime;
    }

    /**
     * 加载任务元信息
     * @author xiaoqianbin
     * @date 2020/7/13
     **/
    protected void loadTaskMetas() {
        RegistryHelper helper = getRegistryHelper();
        String taskPath = helper.getRootPath() + RegistryHelper.TASKS_META_USERS;
        List<String> children = helper.getClient().getChildren(taskPath);
        taskMetaMap.clear();
        for (String child : children) {
            TaskMeta meta = helper.getClient().readData(taskPath + PS + child);
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
     * @author xiaoqianbin
     * @date 2020/7/13
     **/
    protected void recoverUnFinishedTasks() {
        RegistryHelper helper = getRegistryHelper();
        String scheduleTaskPath = helper.getRootPath() + RegistryHelper.TASKS_EXECUTION_RUNNING;
        List<String> scheduleGroups = helper.getClient().getChildren(scheduleTaskPath);
        for (String group : scheduleGroups) {
            List<String> scheduleTasks = getRegistryHelper().getClient().getChildren(scheduleTaskPath + PS + group);
            if (scheduleTasks.isEmpty()) {
                continue;
            }
            scheduleTasks.sort(String::compareTo);
            // 移除 "/libra/root/tasks/execution/running/{groupName}/{taskName}" 下的脏数据
            removeDirtyRegistryInformation(scheduleTaskPath, group, scheduleTasks);

            // 注册 "/libra/root/tasks/execution/users/{taskName}/{scheduleTime}" 的监听器
            registerTaskExecutionListener(scheduleTaskPath, group, scheduleTasks);
        }
    }

    /**
     * 注册任务执行情况监听器
     * @param scheduleTaskPath
     * @param group
     * @param scheduleTasks
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    private void registerTaskExecutionListener(String scheduleTaskPath, String group, List<String> scheduleTasks) {
        String taskName = scheduleTasks.get(scheduleTasks.size() - 1);
        String path = scheduleTaskPath + PS + group + PS + taskName;
        // 一个任务可能有多个调度在执行
        List<String> scheduleTimes = getRegistryHelper().getClient().getChildren(path);
        for (String scheduleTime : scheduleTimes) {
            String execPath = getRegistryHelper().getRootPath() + RegistryHelper.TASKS_EXECUTION_USERS + PS + taskName + PS + scheduleTime;
            if (getRegistryHelper().getClient().exists(execPath)) {
                // schedule节点中有数据，运行节点没数据
                getRegistryHelper().createPersistNode(execPath);
            }
            IZkChildListener listener = createExecutionListener(group, taskName, scheduleTime);
            if (listenerMap.containsKey(execPath)) {
                getRegistryHelper().getClient().unsubscribeChildChanges(execPath, listenerMap.get(execPath));
            }
            listenerMap.put(execPath, listener);
            getRegistryHelper().getClient().subscribeChildChanges(execPath, listener);
            // 检测下任务的完成状态，如果完成了，需要更新下执行节点
            checkSchedulingStatus(group, taskName, scheduleTime);
        }
    }

    /**
     * 创建一个节点监听器
     * @param group
     * @param taskName
     * @param scheduleTime
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    private IZkChildListener createExecutionListener(String group, String taskName, String scheduleTime) {
        return (nodePath, list) -> {
            checkSchedulingStatus(group, taskName, scheduleTime);
        };
    }

    /**
     * 检测下任务的完成状态，如果完成了，需要更新下执行节点
     * @param group
     * @param taskName
     * @param scheduleTime
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    private void checkSchedulingStatus(String group, String taskName, String scheduleTime) {
        String execPath = getRegistryHelper().getRootPath() + RegistryHelper.TASKS_EXECUTION_USERS + PS + taskName + PS + scheduleTime;
        synchronized (listenerMap.get(execPath)) {
            List<String> children = getRegistryHelper().getClient().getChildren(execPath);
            Map<Boolean, List<String>> statusMap = children.stream().collect(Collectors.groupingBy(s -> s.startsWith(RUNNING_TASK_PREFIX)));
            int splitsCount = taskMetaMap.get(group).stream().filter(t -> t.getTaskName().equals(taskName)).collect(Collectors.toList()).get(0).getSplitsCount();
            if (statusMap.containsKey(false) && statusMap.get(false).size() != splitsCount) {
                // 还有未完成的分片 直接跳过
                return;
            }
            getRegistryHelper().getClient().unsubscribeChildChanges(execPath, listenerMap.remove(execPath));
            String nextTask = getNextTask(group, taskName);
            String runningRoot = getRegistryHelper().getRootPath() + RegistryHelper.TASKS_EXECUTION_RUNNING + PS + group;
            if (null != nextTask) {
                // 调度分组中的下一个任务
                getRegistryHelper().createPersistNode(runningRoot + PS + scheduleTime + PS + nextTask);
                removeLastTaskScheduleInfo(taskName, scheduleTime, runningRoot);
                String nextExecPath = getRegistryHelper().getRootPath() + RegistryHelper.TASKS_EXECUTION_USERS + PS + scheduleTime + PS + nextTask;
                getRegistryHelper().createPersistNode(nextExecPath);
                // 注册下个节点的监听事件
                IZkChildListener listener = createExecutionListener(group, nextTask, scheduleTime);
                listenerMap.put(nextExecPath, listener);
                getRegistryHelper().getClient().subscribeChildChanges(nextExecPath, listener);
            } else {
                removeLastTaskScheduleInfo(taskName, scheduleTime, runningRoot);
            }
        }
    }

    /**
     * 移除已完成的节点schedule信息
     * @param taskName     任务名
     * @param scheduleTime scheduleTime
     * @param runningRoot  {rootPath} + "/tasks/execution/running/" + group
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    private void removeLastTaskScheduleInfo(String taskName, String scheduleTime, String runningRoot) {
        getRegistryHelper().deleteNode(runningRoot + PS + scheduleTime + PS + taskName);
        if (getRegistryHelper().getClient().getChildren(runningRoot + scheduleTime).isEmpty()) {
            getRegistryHelper().deleteNode(runningRoot + PS + scheduleTime);
        }
    }

    /**
     * 获取分组中的下一个任务
     * @param group
     * @param taskName
     * @author xiaoqianbin
     * @date 2020/7/14
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
     * @param runningTaskPath
     * @param group
     * @param scheduleTasks
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    private void removeDirtyRegistryInformation(String runningTaskPath, String group, List<String> scheduleTasks) {
        if (scheduleTasks.size() > 1) {
            for (int i = 0; i < scheduleTasks.size() - 1; i++) {
                getRegistryHelper().getClient().delete(runningTaskPath + PS + group + PS + scheduleTasks.get(i));
            }
        }
    }

    @Override
    protected final String getTaskGroup() {
        return SCHEDULE_GROUP;
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
        intervalSemaphore.release();
        scheduleSemaphore.release();
    }

    @Override
    protected String getCronExpression() {
        return "0 0 0 * * *";
    }
}
