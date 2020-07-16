package rabbit.open.libra.client.task;

import org.apache.zookeeper.CreateMode;
import org.springframework.util.CollectionUtils;
import rabbit.open.libra.client.AbstractLibraTask;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.exception.LibraException;
import rabbit.open.libra.client.execution.ExecutableTask;
import rabbit.open.libra.client.execution.ExecutionMeta;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 用户分布式任务
 * @author xiaoqianbin
 * @date 2020/7/13
 **/
public abstract class DistributedTask extends AbstractLibraTask {

    // /libra/root/tasks/execution/users/{getTaskName()}
    private String taskNodePath;

    // 任务队列
    private ArrayBlockingQueue<ExecutableTask> taskList;

    private Semaphore taskSemaphore;

    private Semaphore quitSemaphore = new Semaphore(0);

    private boolean run = true;

    private List<Thread> executors = new ArrayList<>();

    /**
     * 初始化任务线程
     * @author  xiaoqianbin
     * @date    2020/7/14
     **/
    public DistributedTask() {
        taskSemaphore = new Semaphore(getParallel());
        taskList = new ArrayBlockingQueue<>(getParallel());
        for (int i = 0; i < getParallel(); i++) {
            Thread executor = new Thread(new Runnable() {

                // 空转计数器
                private int counter = 0;

                @Override
                public void run() {
                    while (run) {
                        ExecutableTask task = null;
                        try {
                            task = taskList.poll(3, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            logger.error(e.getMessage(), e);
                        }
                        if (null != task) {
                            executeUserTask(task);
                            counter = 0;
                        } else {
                            counter++;
                            if (20 == counter) {
                                counter = 0;
                                tryAcquireTask();
                            }
                        }
                    }
                }
            }, getTaskName() + "-" + i);
            executor.setDaemon(false);
            executor.start();
            executors.add(executor);
        };
    }

    /**
     * 运行用户任务
     * @param	task
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    private void executeUserTask(ExecutableTask task) {
        try {
            logger.info("executing task [{} - {}]", task.getPath(), task.getNode());
            task.run();
            taskSemaphore.release();
            // 新增运行完成的分片节点
            logger.info("task[{}/{}] is finished", task.getPath(), task.getNode().substring(RUNNING_TASK_PREFIX.length()));
            getRegistryHelper().createPersistNode(task.getPath() + PS + task.getNode().substring(RUNNING_TASK_PREFIX.length()));
            // 删除运行中的分片节点
            getRegistryHelper().deleteNode(task.getPath() + PS + task.getNode());
            String[] split = task.getPath().split(PS);
            Map<Boolean, List<String>> executeInfo = getExecuteInfo(split[split.length - 1]);
            if (executeInfo.containsKey(false) && executeInfo.get(false).size() == getSplitsCount()) {
                logger.info("{} finished", getTaskName());
            }
        } catch (LibraException e) {
            logger.error(e.getMessage());
            taskSemaphore.release();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            taskSemaphore.release();
            // 任务出现异常 删除运行中的分片节点 （推进重试）
            getRegistryHelper().deleteNode(task.getPath() + PS + task.getNode());
        }
        if (!quitSemaphore.tryAcquire()) {
            tryAcquireTask();
        }
    }

    @Override
    protected void close() {
        run = false;
        quitSemaphore.release();
        for (Thread executor : executors) {
            try {
                executor.join();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            logger.info("executor[{}] closed", executor.getName());
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        String sysNode = getRegistryHelper().getRootPath() + RegistryHelper.TASKS_EXECUTION_USERS;
        taskNodePath = sysNode + PS + getTaskName();
        registerTaskExecutionNode(taskNodePath);
        // 监听任务发布信息
        getRegistryHelper().getClient().subscribeChildChanges(taskNodePath, (path, list) -> {
            if (!CollectionUtils.isEmpty(list)) {
                tryAcquireTask();
            }
        });
        tryAcquireTask();
    }

    /**
     * 执行任务
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    private synchronized void tryAcquireTask() {
        if (0 == taskSemaphore.availablePermits()) {
            // 节点没有多余的资源，不执行任务
            return;
        }
        List<String> tasks = getRegistryHelper().getClient().getChildren(taskNodePath);
        if (CollectionUtils.isEmpty(tasks)) {
            // 没有处于调度状态的任务，不执行任务
            return;
        }
        tryAcquireUnFinishedTasks(tasks);
    }

    /**
     * 尝试获取处于调度状态的任务
     * @param	tasks
     * @author  xiaoqianbin
     * @date    2020/7/14
     **/
    private void tryAcquireUnFinishedTasks(List<String> tasks) {
        tasks.sort(String::compareTo);
        for (String task : tasks) {
            Map<Boolean, List<String>> groups = getExecuteInfo(task);
            List<String> leftPieces = getAvailablePieces(groups);
            for (String piece : leftPieces) {
                if (taskSemaphore.availablePermits() > 0 && try2AcquireControl(taskNodePath + PS + task + PS + RUNNING_TASK_PREFIX + piece,
                        new ExecutionMeta(new Date(), null , getTaskName()),
                        CreateMode.EPHEMERAL)) {
                    deductPermits();
                    addTask(taskNodePath + PS + task, RUNNING_TASK_PREFIX + piece, task);
                }
            }
        }
    }

    /**
     * 获取任务运行情况
     * @param	task
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    protected Map<Boolean, List<String>> getExecuteInfo(String task) {
        List<String> children = getRegistryHelper().getClient().getChildren(taskNodePath + PS + task);
        return children.stream().collect(Collectors.groupingBy(s -> s.startsWith(RUNNING_TASK_PREFIX)));
    }

    /**
     * 添加任务到任务列表
     * @param	path
	 * @param	node
	 * @param	executeTime
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    private void addTask(String path, String node, String executeTime) {
        try {
            taskList.put(new ExecutableTask(() -> {
                execute(Integer.parseInt(node.substring(RUNNING_TASK_PREFIX.length())), getSplitsCount(), executeTime);
            }, path, node));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 减少信号量
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    private void deductPermits() {
        try {
            taskSemaphore.acquire(1);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 获取可执行的切片
     * @param	groups
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    private List<String> getAvailablePieces(Map<Boolean, List<String>> groups) {
        List<String> leftPieces = new ArrayList<>();
        for (int i = 0; i < getSplitsCount(); i++) {
            leftPieces.add(i + "");
        }
        if (groups.containsKey(false)) {
            leftPieces.removeAll(groups.get(false));
        }
        if (groups.containsKey(true)) {
            for (String s : groups.get(true)) {
                leftPieces.remove(s.substring(RUNNING_TASK_PREFIX.length()));
            }
        }
        return leftPieces;
    }


    /**
     * 注册任务执行节点
     * @param	nodePath
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    private void registerTaskExecutionNode(String nodePath) {
        try {
            getRegistryHelper().getClient().create(nodePath, null, CreateMode.PERSISTENT);
        } catch (Exception e) {
            // TO DO: ignore
        }
    }
}
