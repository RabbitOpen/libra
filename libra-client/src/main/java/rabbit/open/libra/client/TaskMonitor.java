package rabbit.open.libra.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import rabbit.open.libra.client.meta.TaskMeta;
import rabbit.open.libra.client.task.DistributedTask;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 任务监听器
 * @author xiaoqianbin
 * @date 2020/8/14
 **/
public class TaskMonitor {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Value("${libra.task.runner.corePoolSize:10}")
    private int coreRunnerSize;

    @Value("${libra.task.runner.maxPoolSize:10}")
    private int maxRunnerSize;

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
    private List<DistributedTask> tasks = new ArrayList<>();

    /**
     * 任务map
     **/
    private Map<String, DistributedTask> taskMap = new ConcurrentHashMap<>();

    /**
     * 待扫描路径队列
     **/
    private ArrayBlockingQueue<String> scanPathQueue = new ArrayBlockingQueue<>(maxMonitorTaskSize);

    @Autowired
    RegistryHelper helper;

    private Semaphore loaderCloseSemaphore = new Semaphore(0);

    private ReentrantLock pathLock = new ReentrantLock();

    /**
     * 任务加载线程
     **/
    private ThreadPoolExecutor taskLoader;

    /**
     * 任务执行线程
     **/
    private ThreadPoolExecutor taskRunner;

    @PostConstruct
    public void init() {
        taskRunner = new ThreadPoolExecutor(coreRunnerSize, maxRunnerSize, 30,
                TimeUnit.MINUTES, new ArrayBlockingQueue<>(taskQueueSize));
        taskLoader = new ThreadPoolExecutor(3, 3, 30,
                TimeUnit.MINUTES, new ArrayBlockingQueue<>(taskQueueSize));
        taskLoader.submit(() -> {
            while (true) {
                try {
                    if (loaderCloseSemaphore.tryAcquire(3, TimeUnit.SECONDS)) {
                        break;
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
                loadTask();
            }
        });
    }

    /**
     * 加载任务
     * @author  xiaoqianbin
     * @date    2020/8/14
     **/
    private void loadTask() {
        String taskPath = poll();
        if (null == taskPath) {
            return;
        }
    }

    /**
     * 任务注册
     * @param	task
     * @author  xiaoqianbin
     * @date    2020/8/14
     **/
    public void register(DistributedTask task) {
        tasks.add(task);
        String name = task.getAppName() + Task.SP + task.getTaskName();
        helper.registerTaskMeta(name, new TaskMeta(task), false);
        // 任务节点相对路径
        String relativeTaskPath = RegistryHelper.META_TASKS + Task.SP + name;
        // 任务节点绝对路径
        String taskNodePath = helper.getNamespace() + relativeTaskPath;
        taskMap.put(taskNodePath, task);
        helper.subscribeChildChanges(relativeTaskPath, (path, children) -> addScanPath(path));
        addScanPath(taskNodePath);
    }

    /**
     * 取出待扫描路径
     * @author  xiaoqianbin
     * @date    2020/8/14
     **/
    public String poll() {
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
    public void addScanPath(String path) {
        pathLock.lock();
        try {
            if (!scanPathQueue.contains(path)) {
                scanPathQueue.put(path);
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
        loaderCloseSemaphore.release(10);
        taskLoader.shutdown();
        taskRunner.shutdown();
        logger.info("task monitor is closed");
    }
}
