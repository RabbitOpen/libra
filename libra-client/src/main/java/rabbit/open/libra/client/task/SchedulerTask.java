package rabbit.open.libra.client.task;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;
import rabbit.open.libra.client.AbstractLibraTask;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.ScheduleType;
import rabbit.open.libra.client.TaskMeta;
import rabbit.open.libra.client.meta.ScheduleContext;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * 调度任务，分布式任务的调度核心，负责调度所有其他任务
 * @author xiaoqianbin
 * @date 2020/7/11
 **/
public class SchedulerTask extends AbstractLibraTask {

    /**
     * 调度组
     **/
    public static final String SCHEDULE_GROUP = "SYSTEM";

    /**
     * 默认并行任务组个数
     **/
    public static final int GROUP_TASK_CONCURRENCE = 2;

    /**
     * 默认保留历史运行状态个数
     **/
    public static final int HISTORY_REPLICATION_COUNT = 5;

    /**
     * 调度线程
     **/
    private Thread schedulerThread;

    /**
     * leader   节点
     **/
    private boolean leader = false;

    @Autowired
    protected RegistryHelper helper;

    protected AtomicLong mutex = new AtomicLong(0);

    /**
     * 加载元信息锁
     **/
    protected ReentrantLock metaLoadingLock = new ReentrantLock();

    /**
     * 任务元信息 key: group名
     **/
    protected Map<String, Map<String, List<TaskMeta>>> taskMetaMap = new ConcurrentHashMap<>();

    /**
     * 节点监听器列表
     **/
    private Map<String, IZkChildListener> listenerMap = new ConcurrentHashMap<>();
    
    
    /**
     * 数据变化监听器
     */
    private Map<String, IZkDataListener> dataChangeListenerMap = new ConcurrentHashMap<>();

    /**
     * 监控线程阻塞信号
     **/
    private Semaphore intervalSemaphore = new Semaphore(0);

    /**
     * 调度信号
     **/
    private Semaphore scheduleSemaphore = new Semaphore(0);

    /**
     * 任务组调度时间, 一级key时app，二级是group
     * @author xiaoqianbin
     * @date 2020/7/15
     **/
    private Map<String, Map<String, Date>> groupScheduleMap = new ConcurrentHashMap<>();

    /**
     * 手动任务队列
     **/
    private ArrayBlockingQueue<String> manualTasks = new ArrayBlockingQueue<>(512);

    @Override
    public RegistryHelper getRegistryHelper() {
        return helper;
    }

    /**
     * 执行任务
     * @param context
     * @author xiaoqianbin
     * @date 2020/7/13
     **/
    @Override
    public void execute(ScheduleContext context) {
        if (mutex.getAndAdd(1L) > 1) {
            return;
        }
        String schedulePath = RegistryHelper.TASKS_EXECUTION_SCHEDULE + PS + getTaskName();
        // 注册网络事件监听
        registerStateChangeListener();
        // 抢占控制权
        getRegistryHelper().subscribeChildChanges(RegistryHelper.TASKS_EXECUTION_SCHEDULE, (path, list) -> {
            if (!list.contains(getTaskName())) {
                logger.info("leader is lost");
                try2AcquireControl(schedulePath, CreateMode.EPHEMERAL);
            } else {
                if (getLeaderName().equals(getRegistryHelper().readData(schedulePath))) {
                    leader = true;
                } else {
                    leader = false;
                }
            }
        });
        try2AcquireControl(schedulePath, CreateMode.EPHEMERAL);
        startScheduleThread();
    }

    @Override
    public final String getTaskName() {
        return SchedulerTask.class.getSimpleName();
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
                    if (intervalSemaphore.tryAcquire(3, TimeUnit.SECONDS)) {
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
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    private void registerStateChangeListener() {

        getRegistryHelper().subscribeStateChanges(new IZkStateListener() {

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
                helper.registerExecutor();
                String schedulePath = RegistryHelper.TASKS_EXECUTION_SCHEDULE + PS + getTaskName();
                if (helper.exists(schedulePath)) {
                    if (getLeaderName().equals(helper.readData(schedulePath))) {
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
            public void handleNewSession() {
                // TO DO： ignore
            }

            @Override
            public void handleSessionEstablishmentError(Throwable throwable) {
                // TO DO： ignore
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

        // 打印日志
        logMeta();

        // 添加手动任务监听器
        addManualTaskListener();

        // 尝试恢复未完成的任务
        recoverUnFinishedTasks();

        // 定时调度未被调度的节点
        doSchedule();
    }

    /**
     * 手动任务监听器
     * @author  xiaoqianbin
     * @date    2020/7/24
     **/
    private void addManualTaskListener() {
        if (!dataChangeListenerMap.containsKey(RegistryHelper.TASKS_EXECUTION_TRIGGER)) {
            IZkDataListener manualTaskListener = new IZkDataListener() {
                @Override
                public void handleDataChange(String path, Object o) {
                    manualTaskChanged();
                }

                @Override
                public void handleDataDeleted(String s) {
                    // nobody cares this branch
                }
            };
            dataChangeListenerMap.put(RegistryHelper.TASKS_EXECUTION_TRIGGER, manualTaskListener);
            helper.subscribeDataChanges(RegistryHelper.TASKS_EXECUTION_TRIGGER, manualTaskListener);
            manualTaskChanged();
        }
    }
    
    /**
     * 手动任务更新
     * @author  xiaoqianbin
     * @date    2020/7/24
     **/
    private void manualTaskChanged() {
        List<String> children = helper.getChildren(RegistryHelper.TASKS_EXECUTION_TRIGGER);
        for (String child : children) {
            if (!manualTasks.contains(child)) {
                try {
                    manualTasks.put(child);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        scheduleManualTask();
    }

    /**
     * 调度人工任务
     * @author  xiaoqianbin
     * @date    2020/7/24
     **/
    private void scheduleManualTask() {
        while (true) {
            String task = manualTasks.poll();
            if (null == task) {
                break;
            } else {
                String[] description = task.split("@");
                if (5 != description.length) {
                    logger.error("wrong manual task description[{}] is found", task);
                } else {
                    publishManualTask(description, task);
                }
            }
        }
    }

    /**
     * 发布人工任务
     * @param	description
     * @param	task
     * @author  xiaoqianbin
     * @date    2020/7/24
     **/
    private void publishManualTask(String[] description, String task) {
        String appName = description[0];
        String group = description[1];
        String taskName = description[2];
        String schedule = description[3];
        String type = description[4];
        String groupRunningPath = RegistryHelper.TASKS_EXECUTION_RUNNING + PS + appName + PS + group;
        String taskPath = RegistryHelper.TASKS_EXECUTION_USERS + PS + appName + PS + taskName;
        logger.info("task group[{}] is scheduled at [{}]", group, schedule);
        helper.createPersistNode(groupRunningPath + PS + schedule + PS + taskName);
        // 创建执行信息
        onCreateExecuteNode(appName, group, taskName, schedule);
        helper.createPersistNode(taskPath + PS + schedule, ScheduleType.valueOf(type), true);
        postPublish(appName, group, taskName, schedule);
        addExecutionListener(group, taskName, schedule, appName);
        helper.deleteNode(RegistryHelper.TASKS_EXECUTION_TRIGGER + PS + task);
    }

    /**
     * 创建执行节点前置操作
     * @param	appName
	 * @param	group
	 * @param	taskName
	 * @param	schedule
     * @author  xiaoqianbin
     * @date    2020/7/29
     **/
    private void onCreateExecuteNode(String appName, String group, String taskName, String schedule) {
        prePublish(appName, group, taskName, schedule);
        onTaskStarted(appName, group, taskName, schedule);
    }

    /**
     * 打印meta信息
     * @author  xiaoqianbin
     * @date    2020/7/16
     **/
    private void logMeta() {
        // print logs
        for (Map.Entry<String, Map<String, List<TaskMeta>>> entry : taskMetaMap.entrySet()) {
            logger.info("found app tasks, app: [{}], tasks: {}", entry.getKey(), entry.getValue());
        }
    }

    /**
     * 定时调度的任务节点
     * @author xiaoqianbin
     * @date 2020/7/15
     **/
    protected void doSchedule() {
        logger.info("scheduler job is running....");
        while (true) {
            try {
                for (Map.Entry<String, Map<String, List<TaskMeta>>> appMeta : taskMetaMap.entrySet()) {
                    publishByApp(appMeta.getKey(), appMeta.getValue());
                }
            } catch (Exception e) {
                logger.info(e.getMessage(), e);
            }
            if (!leader || waits(3)) {
                // 如果leader不是自己, 或者信号唤醒了
                break;
            }
        }
        logger.info("scheduler job is stopped!");
    }

    /**
     * 按应用发布
     * @param appName
     * @param appMeta
     * @author xiaoqianbin
     * @date 2020/7/16
     **/
    protected void publishByApp(String appName, Map<String, List<TaskMeta>> appMeta) throws ParseException {
        for (Map.Entry<String, List<TaskMeta>> entry : appMeta.entrySet()) {
            if (SCHEDULE_GROUP.equals(entry.getKey())) {
                continue;
            }
            try2PublishTaskGroup(appName, entry.getValue());
        }
    }

    /**
     * 等待
     * @param seconds
     * @author xiaoqianbin
     * @date 2020/7/16
     **/
    private boolean waits(int seconds) {
        try {
            return scheduleSemaphore.tryAcquire(seconds, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error(e.getMessage());
            return false;
        }
    }

    /**
     * 尝试发布任务组
     * @param appName
     * @param groupMetas
     * @author xiaoqianbin
     * @date 2020/7/15
     **/
    protected void try2PublishTaskGroup(String appName, List<TaskMeta> groupMetas) throws ParseException {
        if (groupMetas.isEmpty()) {
            return;
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String taskName = groupMetas.get(0).getTaskName();
        String group = groupMetas.get(0).getGroupName();
        Date nextScheduleTime = getNextScheduleTime(appName, groupMetas);
        if (nextScheduleTime.before(new Date())) {
            String schedulePath = RegistryHelper.TASKS_EXECUTION_SCHEDULE + PS + getTaskName();
            String currentLeader = getRegistryHelper().readData(schedulePath);
            String taskPath = RegistryHelper.TASKS_EXECUTION_USERS + PS + appName + PS + taskName;
            if (!getLeaderName().equals(currentLeader)) {
                logger.error("leader is changed! current leader is {}", currentLeader);
                leader = false;
                return;
            }
            if (scheduleTooBusy(appName, group)) {
                logger.warn("group[{} ---> {}] task is blocked", appName, group);
                doHistoryClean(taskPath, appName, taskName, group);
                return;
            }
            String schedule = sdf.format(nextScheduleTime);
            String groupRunningPath = RegistryHelper.TASKS_EXECUTION_RUNNING + PS + appName + PS + group;
            //创建进度信息
            helper.createPersistNode(groupRunningPath + PS + schedule + PS + taskName);
            logger.info("task group[{}] is scheduled at [{}]", group, schedule);
            String executePath = taskPath + PS + schedule;
            // 创建执行信息
            onCreateExecuteNode(appName, group, taskName, schedule);
            helper.createPersistNode(executePath);
            postPublish(appName, group, taskName, schedule);
            addExecutionListener(group, taskName, schedule, appName);
            groupScheduleMap.remove(group);
            doHistoryClean(taskPath, appName, taskName, group);
        }
    }

    /**
     * 清理执行完毕的历史副本
     * @param	taskPath
     * @param	appName
     * @param	taskName
     * @param	groupName
     * @author  xiaoqianbin
     * @date    2020/7/16
     **/
    protected void doHistoryClean(String taskPath, String appName, String taskName, String groupName) {
        TaskMeta taskMeta = taskMetaMap.get(appName).get(groupName).stream().filter(t -> taskName.equals(t.getTaskName())).collect(Collectors.toList()).get(0);
        List<String> historyTasks = helper.getChildren(taskPath);
        historyTasks.sort(String::compareTo);
        int total = historyTasks.size();
        int removed = 0;
        for (String historyTask : historyTasks) {
            if (removed >= total - getHistoryReplicationCount()) {
                break;
            }
            List<String> children = getRegistryHelper().getChildren(taskPath + PS + historyTask);
            Map<Boolean, List<String>> map = children.stream().collect(Collectors.groupingBy(s -> s.startsWith(RUNNING_TASK_PREFIX)));
            if (!CollectionUtils.isEmpty(map.get(false)) && taskMeta.getSplitsCount() == map.get(false).size()) {
                // 任务节点所有分片都已完成
                try {
                    helper.deleteNode(taskPath + PS + historyTask);
                    removed++;
                } catch (Exception e) {
                    logger.warn(e.getMessage());
                }
            }
        }
    }

    /**
     * 调度太频繁，任务阻塞
     * @param appName
     * @param group
     * @author xiaoqianbin
     * @date 2020/7/16
     **/
    private boolean scheduleTooBusy(String appName, String group) {
        String groupRunningPath = RegistryHelper.TASKS_EXECUTION_RUNNING + PS + appName + PS + group;
        if (helper.exists(groupRunningPath)) {
            List<String> children = helper.getChildren(groupRunningPath);
            if (!CollectionUtils.isEmpty(children) && children.size() >= getGroupTaskConcurrence()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 获取任务组下次调度的时间
     * @param appName
     * @param groupMetas
     * @author xiaoqianbin
     * @date 2020/7/15
     **/
    protected Date getNextScheduleTime(String appName, List<TaskMeta> groupMetas) throws ParseException {
        if (!groupScheduleMap.containsKey(appName)) {
            groupScheduleMap.put(appName, new ConcurrentHashMap<>());
        }
        String group = groupMetas.get(0).getGroupName();
        String taskName = groupMetas.get(0).getTaskName();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        if (groupScheduleMap.get(appName).containsKey(group)) {
            // 下次任务的执行时间已经生成了，但是还没有到调度期
            String scheduleTask = RegistryHelper.TASKS_EXECUTION_USERS + PS + appName + PS + taskName + PS
                    + sdf.format(groupScheduleMap.get(appName).get(group));
            if (!helper.exists(scheduleTask)) {
                return groupScheduleMap.get(appName).get(group);
            }
        }
        // 历史schedule
        List<String> historySchedules = helper.getChildren(RegistryHelper.TASKS_EXECUTION_USERS + PS + appName + PS + taskName);
        Date nextScheduleTime;
        if (CollectionUtils.isEmpty(historySchedules)) {
            nextScheduleTime = groupMetas.get(0).getNextScheduleTime(null);
        } else {
            historySchedules.sort(String::compareTo);
            nextScheduleTime = groupMetas.get(0).getNextScheduleTime(sdf.parse(historySchedules.get(historySchedules.size() - 1)));
        }
        groupScheduleMap.get(appName).put(group, nextScheduleTime);
        return nextScheduleTime;
    }

    /**
     * 加载任务元信息
     * @author xiaoqianbin
     * @date 2020/7/13
     **/
    protected void loadTaskMetas() {
        metaLoadingLock.lock();
        try {
            addAppChangeListener();
            List<String> apps = helper.getChildren(RegistryHelper.TASKS_META_USERS);
            Map<String, Map<String, List<TaskMeta>>> appMetaMap = new ConcurrentHashMap<>();
            for (String app : apps) {
                appMetaMap.put(app, loadTaskMetaByApp(app));
            }
            taskMetaMap = appMetaMap;
        } finally {
            metaLoadingLock.unlock();
        }
    }

    /**
     * 按应用加载元信息
	 * @param	appName
     * @author  xiaoqianbin
     * @date    2020/7/17
     **/
    protected Map<String, List<TaskMeta>> loadTaskMetaByApp(String appName) {
        String appPath = RegistryHelper.TASKS_META_USERS;
        Map<String, List<TaskMeta>> metaMap = new ConcurrentHashMap<>(64);
        List<String> tasks = helper.getChildren(appPath + PS + appName);
        for (String task : tasks) {
            TaskMeta meta = helper.readData(appPath + PS + appName + PS + task);
            if (!metaMap.containsKey(meta.getGroupName())) {
                metaMap.put(meta.getGroupName(), new ArrayList<>());
            }
            metaMap.get(meta.getGroupName()).add(meta);
            // 排序
            metaMap.get(meta.getGroupName()).sort(Comparator.comparing(TaskMeta::getExecuteOrder)
                    .thenComparing(TaskMeta::getTaskName));
        }
        return metaMap;
    }

    /**
     * 注册app 动态变更事件
     * @author  xiaoqianbin
     * @date    2020/7/17
     **/
    protected void addAppChangeListener() {
        if (listenerMap.containsKey(RegistryHelper.TASKS_META_USERS)) {
            return;
        }
        IZkChildListener appChangeListener = (path, list) -> appChanged(list);
        listenerMap.put(RegistryHelper.TASKS_META_USERS, appChangeListener);
        helper.subscribeChildChanges(RegistryHelper.TASKS_META_USERS, appChangeListener);
        List<String> apps = helper.getChildren(RegistryHelper.TASKS_META_USERS);
        appChanged(apps);
    }

    /**
     * 监听app
     * @param	appList
     * @author  xiaoqianbin
     * @date    2020/7/17
     **/
    protected void appChanged(List<String> appList) {
        synchronized (helper) {
            if (!CollectionUtils.isEmpty(appList)) {
                for (String app : appList) {
                    String appPath = RegistryHelper.TASKS_META_USERS + PS + app;
                    if (listenerMap.containsKey(appPath)) {
                        continue;
                    }
                    addTaskChangeListener(appPath, app);
                    appTaskChanged(app);
                }
                logger.info("app changed: current task meta: {}", taskMetaMap);
            }
        }
    }

    /**
     * 刷新app下的meta
     * @param	app
     * @author  xiaoqianbin
     * @date    2020/7/17
     **/
    protected void appTaskChanged(String app) {
        synchronized (taskMetaMap) {
            Map<String, List<TaskMeta>> map = loadTaskMetaByApp(app);
            taskMetaMap.put(app, map);
            logger.info("app task changed: {}", taskMetaMap);
        }
    }

    /**
     * 添加app下task变化的监听器
     * @param	appPath
     * @param	appName
     * @author  xiaoqianbin
     * @date    2020/7/17
     **/
    protected void addTaskChangeListener(String appPath, String appName) {
        IZkChildListener listener = (path, list) -> appTaskChanged(appName);
        helper.subscribeChildChanges(appPath, listener);
        listenerMap.put(appPath, listener);
    }

    /**
     * 任务恢复
     * @author xiaoqianbin
     * @date 2020/7/13
     **/
    protected void recoverUnFinishedTasks() {
        String scheduleTaskPath = RegistryHelper.TASKS_EXECUTION_RUNNING;
        List<String> apps = helper.getChildren(scheduleTaskPath);
        for (String app : apps) {
            List<String> scheduleGroups = helper.getChildren(scheduleTaskPath + PS + app);
            scheduleGroups.sort(String::compareTo);
            for (String group : scheduleGroups) {
                List<String> unfinishedTasks = getRegistryHelper().getChildren(scheduleTaskPath + PS + app + PS + group);
                if (unfinishedTasks.isEmpty()) {
                    continue;
                }
                unfinishedTasks.sort(String::compareTo);
                // 移除 "/libra/root/tasks/execution/running/{appName}/{groupName}/{scheduleTime}" 下的脏数据
                removeDirtyRegistryInformation(scheduleTaskPath, group, unfinishedTasks, app);

                // 注册 "/libra/root/tasks/execution/users/{appName}/{taskName}/{scheduleTime}" 的监听器
                registerTaskExecutionListener(scheduleTaskPath, group, unfinishedTasks, app);
            }
        }

    }

    /**
     * 注册任务执行情况监听器
     * @param scheduleTaskPath
     * @param group
     * @param scheduleTimes
     * @param appName
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    private void registerTaskExecutionListener(String scheduleTaskPath, String group, List<String> scheduleTimes, String appName) {
        for (String scheduleTime : scheduleTimes) {
            String path = scheduleTaskPath + PS + appName + PS + group + PS + scheduleTime;
            // 一个任务可能有多个调度在执行
            List<String> tasks = getRegistryHelper().getChildren(path);
            if (CollectionUtils.isEmpty(tasks)) {
                helper.deleteNode(path);
                continue;
            }
            String taskName = tasks.get(0);
            String execPath = RegistryHelper.TASKS_EXECUTION_USERS + PS + appName + PS + taskName + PS + scheduleTime;
            if (!getRegistryHelper().exists(execPath)) {
                // schedule节点中有数据，运行节点没数据
                onCreateExecuteNode(appName, group, taskName, scheduleTime);
                helper.createPersistNode(execPath);
                postPublish(appName, group, taskName, scheduleTime);
            }
            addExecutionListener(group, taskName, scheduleTime, appName);
            // 检测下任务的完成状态，如果完成了，需要更新下执行节点
            checkSchedulingStatus(group, taskName, scheduleTime, appName);
        }
    }

    /**
     * 添加任务节点监听器
     * @param group        任务分组
     * @param taskName     任务名
     * @param scheduleTime 调度时间片
     * @param appName      应用名
     * @author xiaoqianbin
     * @date 2020/7/16
     **/
    private void addExecutionListener(String group, String taskName, String scheduleTime, String appName) {
        String execPath = RegistryHelper.TASKS_EXECUTION_USERS + PS + appName + PS + taskName + PS + scheduleTime;
        IZkChildListener listener = createExecutionListener(group, taskName, scheduleTime, appName);
        if (listenerMap.containsKey(execPath)) {
            // 防止重复注册
            getRegistryHelper().unsubscribeChildChanges(execPath, listenerMap.get(execPath));
        }
        listenerMap.put(execPath, listener);
        getRegistryHelper().subscribeChildChanges(execPath, listener);
    }

    /**
     * 创建一个节点监听器
     * @param group
     * @param taskName
     * @param scheduleTime
     * @param appName
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    private IZkChildListener createExecutionListener(String group, String taskName, String scheduleTime, String appName) {
        return (nodePath, list) -> checkSchedulingStatus(group, taskName, scheduleTime, appName);
    }

    /**
     * 检测下任务的完成状态，如果完成了，需要更新下执行节点
     * @param group
     * @param taskName
     * @param scheduleTime
     * @param appName
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    private void checkSchedulingStatus(String group, String taskName, String scheduleTime, String appName) {
        String execPath = RegistryHelper.TASKS_EXECUTION_USERS + PS + appName + PS + taskName + PS + scheduleTime;
        IZkChildListener taskListener = listenerMap.get(execPath);
        if (null == taskListener) {
            // 最后一个分片触发的两次事件事件很短暂，第一次就就会处理
            return;
        }
        synchronized (taskListener) {
            if (null == listenerMap.get(execPath)) {
                return;
            }
            List<String> children = getRegistryHelper().getChildren(execPath);
            Map<Boolean, List<String>> statusMap = children.stream().collect(Collectors.groupingBy(s -> s.startsWith(RUNNING_TASK_PREFIX)));
            int splitsCount = taskMetaMap.get(appName).get(group).stream().filter(t -> t.getTaskName().equals(taskName)).collect(Collectors.toList()).get(0).getSplitsCount();
            if (!statusMap.containsKey(false) || statusMap.get(false).size() != splitsCount) {
                // 还有未完成的分片 直接跳过
                return;
            }
            onTaskCompleted(appName, group, taskName, scheduleTime);
            getRegistryHelper().unsubscribeChildChanges(execPath, listenerMap.get(execPath));
            listenerMap.remove(execPath);
            String nextTask = getNextTask(group, taskName, appName);
            String runningRoot = RegistryHelper.TASKS_EXECUTION_RUNNING + PS + appName + PS + group;
            ScheduleType triggerType = helper.readData(execPath);
			if (null != nextTask && ScheduleType.MANUAL_SINGLE != triggerType) {
                // 调度分组中的下一个任务
                getRegistryHelper().createPersistNode(runningRoot + PS + scheduleTime + PS + nextTask);
                removeLastTaskScheduleInfo(taskName, scheduleTime, runningRoot);
                String taskPath = RegistryHelper.TASKS_EXECUTION_USERS + PS + appName + PS + nextTask;
                String nextExecPath = taskPath + PS + scheduleTime;
                onCreateExecuteNode(appName, group, nextTask, scheduleTime);
                helper.createPersistNode(nextExecPath, triggerType, false);
                postPublish(appName, group, nextTask, scheduleTime);
                logger.info("task [{} - {}] is published", nextTask, scheduleTime);
                // 注册下个节点的监听事件
                IZkChildListener listener = createExecutionListener(group, nextTask, scheduleTime, appName);
                listenerMap.put(nextExecPath, listener);
                getRegistryHelper().subscribeChildChanges(nextExecPath, listener);
                doHistoryClean(taskPath, appName, nextTask, group);
            } else {
                removeLastTaskScheduleInfo(taskName, scheduleTime, runningRoot);
                logger.info("task group[{} - {}] is finished", group, scheduleTime);
            }
        }
    }

    /**
     * 保留历史副本个数，（超过这个数就会被清理掉）
     * @author xiaoqianbin
     * @date 2020/7/16
     **/
    protected int getHistoryReplicationCount() {
        return HISTORY_REPLICATION_COUNT;
    }

    /**
     * 并行处理任务 (同一个任务允许出现的并行调度个数)
     * @author xiaoqianbin
     * @date 2020/7/16
     **/
    public int getGroupTaskConcurrence() {
        return GROUP_TASK_CONCURRENCE;
    }

    /**
     * 移除已完成的节点schedule信息
     * @param taskName     任务名
     * @param scheduleTime scheduleTime
     * @param runningRoot  {rootPath} + "/tasks/execution/running/{appName}" + group
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    private void removeLastTaskScheduleInfo(String taskName, String scheduleTime, String runningRoot) {
        getRegistryHelper().deleteNode(runningRoot + PS + scheduleTime + PS + taskName);
        if (getRegistryHelper().getChildren(runningRoot + PS + scheduleTime).isEmpty()) {
            getRegistryHelper().deleteNode(runningRoot + PS + scheduleTime);
        }
    }

    /**
     * 获取分组中的下一个任务
     * @param group
     * @param taskName
     * @param appName
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    private String getNextTask(String group, String taskName, String appName) {
        List<TaskMeta> groupTasks = taskMetaMap.get(appName).get(group);
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
     * @param unfinishedTasks
     * @param appName
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    private void removeDirtyRegistryInformation(String runningTaskPath, String group, List<String> unfinishedTasks, String appName) {
        for (int i = 0; i < unfinishedTasks.size(); i++) {
            String path = runningTaskPath + PS + appName + PS + group + PS + unfinishedTasks.get(i);
            List<String> children = getRegistryHelper().getChildren(path);
            if (CollectionUtils.isEmpty(children)) {
                continue;
            }
            String latestTask = getLatestTask(children, taskMetaMap.get(appName).get(group));
            for (int j = 0; j < children.size(); j++) {
                if (!latestTask.equals(children.get(j))) {
                    getRegistryHelper().delete(path + "/" + children.get(j));
                }
            }
        }
    }

    /**
     * 如果有多个任务在执行进度表中，筛选出最后的那个任务（最后那个才是该运行的）
     * @param tasks
     * @param metaList
     * @author xiaoqianbin
     * @date 2020/7/16
     **/
    protected String getLatestTask(List<String> tasks, List<TaskMeta> metaList) {
        if (1 == tasks.size()) {
            return tasks.get(0);
        }
        String target = "";
        int max = 0;
        for (String task : tasks) {
            for (int i = 0; i < metaList.size(); i++) {
                if (task.equals(metaList.get(i).getTaskName())) {
                    if (i > max) {
                        max = i;
                        target = task;
                    }
                    break;
                }
            }
        }
        return target;
    }

    @Override
    public final String getTaskGroup() {
        return SCHEDULE_GROUP;
    }

    @Override
    protected final boolean isSystemTask() {
        return true;
    }

    @Override
    protected void close() {
        logger.info("scheduler is closing......");
        helper.unsubscribeAll();
        intervalSemaphore.release();
        scheduleSemaphore.release();
        try {
            schedulerThread.join();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 注册任务源信息
     * @author xiaoqianbin
     * @date 2020/7/16
     **/
    @Override
    protected void registerTaskMeta() {
        getRegistryHelper().registerTaskMeta(getTaskName(), new TaskMeta(this), isSystemTask());
    }

    @Override
    protected String getCronExpression() {
        return "0 0 0 * * *";
    }
}
