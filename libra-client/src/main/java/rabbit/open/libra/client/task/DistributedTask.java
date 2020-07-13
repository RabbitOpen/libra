package rabbit.open.libra.client.task;

import org.apache.zookeeper.CreateMode;
import org.springframework.util.CollectionUtils;
import rabbit.open.libra.client.AbstractLibraTask;
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

    // 正在运行的任务的前缀
    public final static String RUNNING_TASK_PREFIX = "R-";

    // 任务队列
    private ArrayBlockingQueue<ExecutableTask> taskList;

    private Semaphore taskSemaphore;

    public DistributedTask() {
        taskSemaphore = new Semaphore(getParallel());
        taskList = new ArrayBlockingQueue<>(getParallel());
        for (int i = 0; i < getParallel(); i++) {
            Thread executor = new Thread(() -> {
                while (true) {
                    ExecutableTask task = null;
                    try {
                        task = taskList.poll(3, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                    if (null != task) {
                        executeUserTask(task);
                    }
                }
            });
            executor.setDaemon(false);
            executor.start();
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
            task.run();
            taskSemaphore.release();
            Map<Boolean, List<String>> executeInfo = getExecuteInfo(task.getPath());
            if (executeInfo.get(false).size() == getSplitsCount()) {
                logger.info("{} finished", getTaskName());
            } else {
                // 继续处理任务
                tryAcquireTask();
            }
            // todo 生成任务完成节点
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            taskSemaphore.release();
        }
    }

    @Override
    protected void close() {

    }

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        String sysNode = getRegistryHelper().getRootPath() + "/tasks/execution/users";
        taskNodePath = sysNode + "/" + getTaskName();
        registerTaskExecutionNode(taskNodePath);
        getRegistryHelper().getClient().subscribeChildChanges(taskNodePath, (path, list) -> {
            logger.info("task [{} - {}] is published", getTaskName(), list);
            tryAcquireTask();
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
            return;
        }
        List<String> tasks = getRegistryHelper().getClient().getChildren(taskNodePath);
        tasks.sort(String::compareTo);
        if (CollectionUtils.isEmpty(tasks)) {
            return;
        }
        for (String task : tasks) {
            Map<Boolean, List<String>> groups = getExecuteInfo(task);
            List<String> leftPieces = getAvailablePieces(groups);
            for (String piece : leftPieces) {
                if (taskSemaphore.availablePermits() > 0 && try2AcquireControl(taskNodePath + "/" + task + "/" + RUNNING_TASK_PREFIX + piece,
                        new ExecutionMeta(new Date(), null , getTaskName()),
                        CreateMode.EPHEMERAL)) {
                    deductPermits();
                    addTask(taskNodePath + "/" + task, RUNNING_TASK_PREFIX + piece, task);
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
    private Map<Boolean, List<String>> getExecuteInfo(String task) {
        List<String> statusList = getRegistryHelper().getClient().getChildren(taskNodePath + "/" + task);
        return statusList.stream().collect(Collectors.groupingBy(s -> s.startsWith(RUNNING_TASK_PREFIX)));
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
        for (int i = 0; i < getParallel(); i++) {
            leftPieces.add(i + "");
        }
        leftPieces.removeAll(groups.get(true));
        for (String s : groups.get(false)) {
            leftPieces.remove(s.substring(RUNNING_TASK_PREFIX.length()));
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
