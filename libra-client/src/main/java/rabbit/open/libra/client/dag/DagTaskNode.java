package rabbit.open.libra.client.dag;

import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.springframework.util.CollectionUtils;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.meta.TaskExecutionContext;
import rabbit.open.libra.client.task.DistributedTask;
import rabbit.open.libra.client.task.SchedulerTask;
import rabbit.open.libra.dag.DagNode;
import rabbit.open.libra.dag.ScheduleStatus;

import java.util.List;
import java.util.UUID;

import static rabbit.open.libra.client.Constant.SP;

/**
 * dag任务节点
 * @author xiaoqianbin
 * @date 2020/8/18
 **/
public class DagTaskNode extends DagNode {

    protected transient SchedulerTask task;

    public static final long serialVersionUID = 1L;

    /**
     * 运行节点的名字
     */
    protected String taskName;

    /**
     * 并行度
     */
    protected int parallel;

    /**
     * 切片个数
     */
    protected int splitsCount;

    /**
     * app name
     */
    protected String appName;

    /**
     * taskId
     **/
    protected String taskId;

    /**
     * 运行上下文
     **/
    protected transient TaskExecutionContext context;

    /**
     * @param taskName    任务名
     * @param parallel    单机并发度
     * @param splitsCount 切片数
     * @param appName     应用名
     */
    public DagTaskNode(String taskName, int parallel, int splitsCount, String appName) {
        super();
        setTaskName(taskName);
        this.parallel = parallel;
        this.splitsCount = splitsCount;
        this.appName = appName;
    }

    public DagTaskNode(DistributedTask task) {
        this(task.getTaskName(), task.getParallel(), task.getSplitsCount(), task.getAppName());
    }

    /**
     * <b>@description 执行调度 </b>
     */
    @Override
    protected void doSchedule() {
        generateTaskExecutionContext();
        String taskMetaPath = RegistryHelper.META_TASKS + SP + context.getAppName() + SP + context.getTaskName();
        String taskInstanceRelativePath = taskMetaPath + SP + context.getTaskId();
        if (!task.getRegistryHelper().exists(taskInstanceRelativePath)) {
            task.getRegistryHelper().create(taskInstanceRelativePath, context, CreateMode.PERSISTENT);
            task.onTaskPublished(context);
            notifyChildrenChanged(taskMetaPath);
        }
        // 监听节点执行
        task.monitorTaskExecution(taskInstanceRelativePath, (path, children) -> onTaskSplitCompleted(taskInstanceRelativePath, children));
        // 检测节点是否已经执行完毕了
        List<String> children = task.getRegistryHelper().getChildren(taskInstanceRelativePath);
        onTaskSplitCompleted(taskInstanceRelativePath, children);
    }

    /**
     * 任务节点片执行完毕回调
     * @param	taskInstanceRelativePath
	 * @param	children
     * @author  xiaoqianbin
     * @date    2020/8/26
     **/
    private void onTaskSplitCompleted(String taskInstanceRelativePath, List<String> children) {
        if (!CollectionUtils.isEmpty(children) && isTaskFinished(children)) {
            task.unsubscribeTaskExecution(taskInstanceRelativePath);
            synchronized (this) {
                if (ScheduleStatus.FINISHED != getScheduleStatus()) {
                    task.onTaskCompleted(context);
                    getGraph().onDagNodeExecuted(this);
                }
            }
        }
    }

    /**
     * 通过重写meta信息通知子节点变更了
     * @param    taskMetaPath
     * @author xiaoqianbin
     * @date 2020/8/24
     **/
    private void notifyChildrenChanged(String taskMetaPath) {
        try {
            Stat stat = new Stat();
            RegistryHelper helper = task.getRegistryHelper();
            Object data = helper.readData(taskMetaPath, stat);
            helper.writeData(taskMetaPath, data, stat.getVersion());
        } catch (ZkBadVersionException e) {
            notifyChildrenChanged(taskMetaPath);
        }
    }

    /**
     * <b>@description 生成task运行context信息 </b>
     */
    protected void generateTaskExecutionContext() {
        RuntimeDagInstance graph = getGraph();
        context = new TaskExecutionContext(parallel);
        context.setAppName(appName);
        taskId = UUID.randomUUID().toString().replaceAll("-", "");
        context.setTaskId(taskId);
        context.setContext(graph.getContext());
        context.setSplitsCount(splitsCount);
        context.setScheduleDate(graph.getScheduleDate());
        context.setFireDate(graph.getFireDate());
        context.setTaskName(taskName);
        context.setScheduleId(graph.getScheduleId());
    }

    /**
     * <b>@description 检查任务是否已经完成  </b>
     * @param children
     * @return
     */
    protected boolean isTaskFinished(List<String> children) {
        int finished = 0;
        for (String child : children) {
            if (child.startsWith("R-") || child.startsWith("E-")) {
                return false;
            }
            finished++;
        }
        return finished == context.getSplitsCount();
    }

    @Override
    protected boolean isScheduled() {
        return scheduleStatus == ScheduleStatus.FINISHED;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public void setTask(SchedulerTask task) {
        this.task = task;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getAppName() {
        return appName;
    }
}
