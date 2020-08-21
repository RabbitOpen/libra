package rabbit.open.libra.client.task;

import org.I0Itec.zkclient.IZkDataListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import rabbit.open.libra.client.Constant;
import rabbit.open.libra.client.RegistryConfig;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.ZookeeperMonitor;
import rabbit.open.libra.client.meta.TaskExecutionContext;
import rabbit.open.libra.client.meta.TaskMeta;
import rabbit.open.libra.dag.schedule.ScheduleContext;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * 任务订阅器，负责从zk读取调度器发布的任务
 * @author xiaoqianbin
 * @date 2020/8/14
 **/
public class TaskSubscriber extends ZookeeperMonitor {

    /**
     * 运行任务线程数
     **/
    @Value("${libra.task.runner.corePoolSize:10}")
    private int coreRunnerSize;

    /**
     * 运行任务队列
     **/
    @Value("${libra.task.runner.maxPoolSize:10}")
    private int maxRunnerSize;

    /**
     * 调度任务队列
     **/
    @Value("${libra.task.runner.taskQueueSize:1024}")
    private int taskQueueSize;

    /**
     * 最大同时监控的任务类型个数
     **/
    @Value("${libra.task.runner.maxMonitorTaskSize:4096}")
    private int maxMonitorTaskSize;

    /**
     * 任务列表
     **/
    private List<DistributedTask> distributedTasks = new ArrayList<>();

    // 由于zk异常导致删除失败的节点
    private LinkedBlockingQueue<String> path2remove = new LinkedBlockingQueue<>();

    // 由于zk异常导致添加失败的节点
    private LinkedBlockingQueue<String> path2add = new LinkedBlockingQueue<>();

    /**
     * 任务map, key是task meta信息的相对路径/meta/tasks/{app-name}/{task-name}
     **/
    private Map<String, DistributedTask> taskMap = new ConcurrentHashMap<>();

    /**
     * 任务执行元信息（key是taskId）
     **/
    private Map<String, TaskExecutionContext> taskExecutionMetaMap = new ConcurrentHashMap<>();

    /**
     * 待扫描路径队列
     **/
    private ArrayBlockingQueue<String> scanPathQueue;

    @Autowired
    RegistryConfig config;

    private Semaphore loaderBlockSemaphore = new Semaphore(0);

    private ReentrantLock pathLock = new ReentrantLock();

    /**
     * zk服务端是否就绪
     **/
    protected boolean zkPrepared = true;

    /**
     * 任务加载线程
     **/
    private ThreadPoolExecutor taskLoader;

    private boolean stopTaskLoading = false;

    /**
     * 任务执行线程
     **/
    private ThreadPoolExecutor taskRunner;

    @PostConstruct
    @Override
    public void init() {
        super.init();
        scanPathQueue = new ArrayBlockingQueue<>(maxMonitorTaskSize);
        taskRunner = new ThreadPoolExecutor(coreRunnerSize, maxRunnerSize, 30, TimeUnit.MINUTES, new ArrayBlockingQueue<>(taskQueueSize),
                (r, executor) -> r.run());
        int corePoolSize = 3;
        taskLoader = new ThreadPoolExecutor(corePoolSize, corePoolSize, 30, TimeUnit.MINUTES, new ArrayBlockingQueue<>(taskQueueSize));
        for (int i = 0; i < corePoolSize; i++) {
            submitTaskLoadingJob();
        }
    }

    /**
     * 创建加载任务分片的任务
     * @author  xiaoqianbin
     * @date    2020/8/17
     **/
    private void submitTaskLoadingJob() {
        taskLoader.submit(() -> {
            while (!stopTaskLoading) {
                try {
                    if (loaderBlockSemaphore.tryAcquire(3, TimeUnit.SECONDS) && stopTaskLoading) {
                        break;
                    }
                    loadTask();
                } catch (Exception e) {
                    if (!e.getClass().equals(KeeperException.NoNodeException.class)) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        });
    }

    /**
     * 加载任务
     * @author  xiaoqianbin
     * @date    2020/8/14
     **/
    protected void loadTask() {
        if (!zkPrepared) {
            return;
        }
        while (true) {
            String taskMetaNodePath = getPath2Scan();
            if (null == taskMetaNodePath) {
                return;
            }
            List<String> tasks = helper.getChildren(taskMetaNodePath);
            if (tasks.isEmpty()) {
                continue;
            }
            for (String taskId : tasks) {
                loadTaskById(taskMetaNodePath, taskId);
            }
        }
    }

    /**
     * 按任务id进行任务调度
     * @param	taskMetaNodePath
	 * @param	taskId
     * @author  xiaoqianbin
     * @date    2020/8/15
     **/
    private void loadTaskById(String taskMetaNodePath, String taskId) {
        String taskIdPath = taskMetaNodePath + Constant.SP + taskId;
        TaskExecutionContext meta = getTaskMeta(taskMetaNodePath, taskId);
        if (!meta.hasQuota()) {
            return;
        }
        List<String> existedSplits = getScheduledPieces(helper.getChildren(taskIdPath));
        for (int i = 0; i < meta.getSplitsCount(); i++) {
            if (existedSplits.contains(Integer.toString(i))) {
                continue;
            }
            if (meta.hasQuota() && meta.grabQuota()) {
                if (!try2SubmitTaskPiece(taskMetaNodePath, taskIdPath, meta, i)) {
                    meta.resume();
                }
            } else {
                return;
            }
        }
    }

    /**
     * 获取task meta信息
     * @param	taskMetaNodePath
	 * @param	taskId
     * @author  xiaoqianbin
     * @date    2020/8/15
     **/
    private TaskExecutionContext getTaskMeta(String taskMetaNodePath, String taskId) {
        if (!taskExecutionMetaMap.containsKey(taskId)) {
            synchronized (taskMap.get(taskMetaNodePath)) {
                if (!taskExecutionMetaMap.containsKey(taskId)) {
                    TaskExecutionContext meta = helper.readData(taskMetaNodePath);
                    taskExecutionMetaMap.put(taskId, meta);
                }
            }
        }
        return taskExecutionMetaMap.get(taskId);
    }

    /**
     * 尝试提交任务片
     * @param	taskMetaNodePath
	 * @param	taskIdPath
	 * @param	meta
	 * @param	index
     * @author  xiaoqianbin
     * @date    2020/8/15
     **/
    private boolean try2SubmitTaskPiece(String taskMetaNodePath, String taskIdPath, TaskExecutionContext meta, int index) {
        if (tryAcquireTaskPiece(taskIdPath + "/R-" + index)) {
            taskRunner.submit(() -> {
                try {
                    ScheduleContext context = getContextFromMeta(meta);
                    context.setIndex(index);
                    taskMap.get(taskMetaNodePath).execute(context);
                    createNode(taskIdPath + "/" + index);
                } catch (Exception e) {
                    createNode(taskIdPath + "/E-" + index);
                    logger.error(e.getMessage(), e);
                } finally {
                    removeRunningNode(taskIdPath + "/R-" + index);
                }
                fetchNextTaskPiece(taskMetaNodePath, taskIdPath, meta);
            });
            return true;
        } else {
            return false;
        }
    }

    /**
     * 获取下一个任务片
     * @param	taskMetaNodePath
	 * @param	taskIdPath
	 * @param	meta
     * @author  xiaoqianbin
     * @date    2020/8/15
     **/
    private void fetchNextTaskPiece(String taskMetaNodePath, String taskIdPath, TaskExecutionContext meta) {
        try {
            List<String> pieces = helper.getChildren(taskIdPath);
            List<String> finishedPieces = pieces.stream().filter(s -> !(s.startsWith("R-") || s.startsWith("E-"))).collect(Collectors.toList());
            if (meta.getSplitsCount() == finishedPieces.size()) {
                // 任务结束
                taskExecutionMetaMap.remove(taskIdPath.substring(taskMetaNodePath.length() + 1));
                return;
            }
            List<String> existedSplits = getScheduledPieces(pieces);
            for (int i = 0; i < meta.getSplitsCount(); i++) {
                if (existedSplits.contains(Integer.toString(i))) {
                    continue;
                }
                if (try2SubmitTaskPiece(taskMetaNodePath, taskIdPath, meta, i)) {
                    return;
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void removeRunningNode(String nodePath) {
        try {
            helper.deleteRecursive(nodePath);
        } catch (Exception e) {
            path2remove.add(nodePath);
        }
    }

    /**
     * 创建执行错误节点
     * @param	nodePath
     * @author  xiaoqianbin
     * @date    2020/8/15
     **/
    private void createNode(String nodePath) {
        try {
            helper.create(nodePath, null, CreateMode.PERSISTENT);
        } catch (Exception e) {
            path2add.add(nodePath);
            logger.warn("create node failed -> {}", e.getMessage());
        }
    }

    /**
     * 从meta信息中读取context
     * @param	meta
     * @author  xiaoqianbin
     * @date    2020/8/15
     **/
    private ScheduleContext getContextFromMeta(TaskExecutionContext meta) {
        ScheduleContext context = new ScheduleContext();
        context.setIndex(meta.getIndex());
        context.setScheduleDate(meta.getScheduleDate());
        context.setContext(meta.getContext());
        context.setParallel(meta.getParallel());
        context.setSplitsCount(meta.getSplitsCount());
        context.setFireDate(meta.getFireDate());
        context.setScheduleId(meta.getScheduleId());
        context.setTaskId(meta.getTaskId());
        return context;
    }

    /**
     * 获取已经被调度过的分片
     * @param	list
     * @author  xiaoqianbin
     * @date    2020/8/15
     **/
    private List<String> getScheduledPieces(List<String> list) {
        return list.stream().map(s -> {
                if (s.startsWith("R-") || s.startsWith("E-")) {
                    return s.substring(2);
                }
                return s;
            }).collect(Collectors.toList());
    }

    /**
     * 尝试抢占任务分片
     * @param	nodePath
     * @author  xiaoqianbin
     * @date    2020/8/15
     **/
    private boolean tryAcquireTaskPiece(String nodePath) {
        try {
            helper.create(nodePath, null, CreateMode.EPHEMERAL);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 任务注册
     * @param	task
     * @author  xiaoqianbin
     * @date    2020/8/14
     **/
    public void register(DistributedTask task) {
        distributedTasks.add(task);
        String name = task.getAppName() + Constant.SP + task.getTaskName();
        helper.registerTaskMeta(name, new TaskMeta(task), false);
        // 任务节点相对路径
        String relativeTaskMetaNodePath = RegistryHelper.META_TASKS + Constant.SP + name;
        // 任务节点绝对路径
        String taskNodePath = helper.getNamespace() + relativeTaskMetaNodePath;
        taskMap.put(taskNodePath, task);
        helper.subscribeDataChanges(relativeTaskMetaNodePath, new IZkDataListener() {

            @Override
            public void handleDataChange(String path, Object data) {
                addScanPath(path.substring(helper.getNamespace().length()));
            }

            @Override
            public void handleDataDeleted(String path) {
                // to do: ignore
            }
        });
        addScanPath(taskNodePath.substring(helper.getNamespace().length()));
    }

    /**
     * 取出待扫描路径
     * @author  xiaoqianbin
     * @date    2020/8/14
     **/
    private String getPath2Scan() {
        pathLock.lock();
        try {
            return scanPathQueue.poll();
        } finally {
            pathLock.unlock();
        }
    }

    /**
     * 添加待扫描路径
     * @param	path
     * @author  xiaoqianbin
     * @date    2020/8/14
     **/
    private void addScanPath(String path) {
        pathLock.lock();
        try {
            if (!scanPathQueue.contains(path)) {
                scanPathQueue.put(path);
                loaderBlockSemaphore.release();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            pathLock.unlock();
        }
    }

    @PreDestroy
    public void destroy() {
        logger.info("task monitor is closing");
        stopTaskLoading = true;
        loaderBlockSemaphore.release(10);
        taskLoader.shutdown();
        taskRunner.shutdown();
        getRegistryHelper().destroy();
        logger.info("task monitor is closed");
    }

    @Override
    public RegistryConfig getConfig() {
        return config;
    }

    @Override
    protected void onZookeeperDisconnected() {
        zkPrepared = false;
    }

    @Override
    protected void onZookeeperConnected() {
        if (zkPrepared) {
            return;
        }
        zkPrepared = true;
        // 补偿由于网络问题导致的路径创建失败
        doCompensation(path2add::poll, this::createNode);
        doCompensation(path2remove::poll, this::removeRunningNode);
    }

    /**
     * zk恢复时补偿操作
     * @param	loadPathFunc     路径加载函数
     * @param	funcCompensation 补偿函数
     * @author  xiaoqianbin
     * @date    2020/8/16
     **/
    private void doCompensation(Supplier<String> loadPathFunc, Consumer<String> funcCompensation) {
        logger.info("begin to doCompensation");
        while (true) {
            if (!zkPrepared) {
                return;
            }
            String poll = loadPathFunc.get();
            if (null != poll) {
                funcCompensation.accept(poll);
            } else {
                break;
            }
        }
        logger.info("compensation is finished");
    }
}
