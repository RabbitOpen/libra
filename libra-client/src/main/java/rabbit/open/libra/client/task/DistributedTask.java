package rabbit.open.libra.client.task;

import org.apache.zookeeper.CreateMode;
import org.springframework.util.CollectionUtils;
import rabbit.open.libra.client.AbstractLibraTask;
import rabbit.open.libra.client.ManualScheduleType;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.TaskMeta;
import rabbit.open.libra.client.exception.LibraException;
import rabbit.open.libra.client.execution.ExecutableTask;

import java.text.SimpleDateFormat;
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

	/**
	 * {root path}/tasks/execution/users/{应用名}/{getTaskName} 节点路径
	 **/
	private String taskNodePath;

    // 任务队列
    private ArrayBlockingQueue<ExecutableTask> taskList;

    private Semaphore taskSemaphore;

    private Semaphore quitSemaphore = new Semaphore(0);

    protected boolean run = true;

    /**
     * 任务执行线程
     * @author  xiaoqianbin
     * @date    2020/7/16
     **/
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
            newExecutor(getTaskName() + "-" + i);
        }
    }

    /**
     * 新建任务执行线程
     * @param	executorName
     * @author  xiaoqianbin
     * @date    2020/7/16
     **/
    private void newExecutor(String executorName) {
        Thread executor = new Thread(new Runnable() {

            // 空转计数器
            private int counter = 0;

            @Override
            public void run() {
                while (!isClosed()) {
                    ExecutableTask task = getExecutableTask();
                    try {
                        if (null != task) {
                            executeUserTask(task);
                            counter = 0;
                        } else {
                            counter++;
                            if (30 <= counter) {
                                counter = 0;
                                tryAcquireTask();
                            }
                        }
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }

            /**
             * 获取任务
             * @author  xiaoqianbin
             * @date    2020/7/19
             **/
            private ExecutableTask getExecutableTask() {
                try {
                    return taskList.poll(1, TimeUnit.SECONDS);
                } catch (Exception e) {
                    logger.warn("task[{}] is interrupted", getTaskName());
                    return null;
                }
            }
        }, executorName);
        executor.setDaemon(false);
        executor.start();
        executors.add(executor);
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
            executeTask(task);
            // 新增运行完成的分片节点
            String pieceName = task.getNode().substring(RUNNING_TASK_PREFIX.length());
            logger.info("task[{}/{}] is finished", task.getPath(), pieceName);
            getRegistryHelper().createPersistNode(task.getPath() + PS + pieceName);
            // 删除运行中的分片节点
            getRegistryHelper().deleteNode(task.getPath() + PS + task.getNode());
            String[] split = task.getPath().split(PS);
            Map<Boolean, List<String>> executeInfo = getExecuteInfo(split[split.length - 1]);
            onTaskCompleted(getAppName(), getTaskGroup(), getTaskName(), split[split.length - 1]);
            if (executeInfo.containsKey(false) && executeInfo.get(false).size() == getSplitsCount()) {
                logger.info("{} finished", getTaskName());
            }
        } catch (LibraException e) {
            logger.error(e.getMessage());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            // 任务出现异常 删除运行中的分片节点 （推进重试）
            getRegistryHelper().deleteNode(task.getPath() + PS + task.getNode());
        }
        if (!quitSemaphore.tryAcquire()) {
            tryAcquireTask();
        }
    }

    /**
     * 执行任务
     * @param	task
     * @author  xiaoqianbin
     * @date    2020/7/19
     **/
    private void executeTask(ExecutableTask task) {
        try {
            task.run();
        } finally {
            taskSemaphore.release();
        }
    }

    /**
     * 判断任务是否已经关闭
     * @author  xiaoqianbin
     * @date    2020/7/19
     **/
    protected boolean isClosed() {
        return !run;
    }

    @Override
    protected void close() {
        run = false;
        quitSemaphore.release(getParallel());
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
        taskNodePath = RegistryHelper.TASKS_EXECUTION_USERS + PS + getAppName() + PS + getTaskName();
        registerTaskExecutionNode(taskNodePath);
        // 监听任务发布信息
        getRegistryHelper().subscribeChildChanges(taskNodePath, (path, list) -> {
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
        List<String> schedules = getRegistryHelper().getChildren(taskNodePath);
        if (CollectionUtils.isEmpty(schedules)) {
            // 没有处于调度状态的任务，不执行任务
            return;
        }
        tryAcquireUnFinishedTasks(schedules);
    }

    /**
     * 尝试获取处于调度状态的任务
     * @param	scheduleTImes
     * @author  xiaoqianbin
     * @date    2020/7/14
     **/
    private void tryAcquireUnFinishedTasks(List<String> scheduleTImes) {
		scheduleTImes.sort(String::compareTo);
		for (int i = 0; i < scheduleTImes.size(); i++) {
			String scheduleTime = scheduleTImes.get(i);
			String path = taskNodePath + PS + scheduleTime;
			if (!executeImmediately(scheduleTime)) {
				continue;
			}
			try {
				Map<Boolean, List<String>> groups = getExecuteInfo(scheduleTime);
				List<String> leftPieces = getAvailablePieces(groups);
				for (String piece : leftPieces) {
					if (taskSemaphore.availablePermits() > 0
							&& try2AcquireControl(path + PS + RUNNING_TASK_PREFIX + piece, null, CreateMode.EPHEMERAL)) {
						deductPermits();
						addTask(path, RUNNING_TASK_PREFIX + piece, scheduleTime);
					}
				}
			} catch (LibraException e) {
				// to do: ignore, 清理任务会删除节点，getExecuteInfo有概率抛出节点丢失的异常，直接忽略即可
			} catch (Exception e) {
				logger.error(e.getMessage());
			}
		}
    }

	/**
	 * <b>@description 判断是否需要立即调度任务 </b>
	 * @param task
	 * @return
	 */
	protected boolean executeImmediately(String task) {
		ManualScheduleType type = getRegistryHelper().readData(taskNodePath + PS + task);
		if (null != type) {
			// 手动触发的任务都是立刻执行，忽略掉本身的调度时间
			return true;
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		try {
			Date nextScheduleTime = TaskMeta.getNextScheduleTime(sdf.parse(task), getCronExpression());
			return sdf.format(nextScheduleTime).compareTo(sdf.format(new Date())) <= 0;
		} catch (Exception e) {
			throw new LibraException(e.getMessage());
		}
		
	}
	
    /**
     * 获取任务运行情况
     * @param	task
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    protected Map<Boolean, List<String>> getExecuteInfo(String task) {
        try {
            List<String> children = getRegistryHelper().getChildren(taskNodePath + PS + task);
            return children.stream().collect(Collectors.groupingBy(s -> s.startsWith(RUNNING_TASK_PREFIX)));
        } catch (Exception e) {
            throw new LibraException(e.getMessage());
        }
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
            taskList.put(new ExecutableTask(() -> execute(Integer.parseInt(node.substring(RUNNING_TASK_PREFIX.length())), getSplitsCount(), executeTime), path, node));
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
            leftPieces.add(Integer.toString(i));
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
        getRegistryHelper().createPersistNode(nodePath, true);
    }
}
