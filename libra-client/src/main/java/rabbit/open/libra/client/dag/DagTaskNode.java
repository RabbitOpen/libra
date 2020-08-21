package rabbit.open.libra.client.dag;

import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.meta.TaskExecutionContext;
import rabbit.open.libra.client.task.SchedulerTask;
import rabbit.open.libra.dag.DagNode;
import rabbit.open.libra.dag.ScheduleStatus;

import static rabbit.open.libra.client.Constant.SP;

/**
 * dag任务节点
 * @author xiaoqianbin
 * @date 2020/8/18
 **/
public class DagTaskNode extends DagNode {

    protected transient Logger logger = LoggerFactory.getLogger(getClass());

    protected transient SchedulerTask task;

    /**
     * 运行上下文
     **/
    protected TaskExecutionContext context;

    // 执行调度
    public void doSchedule(SchedulerTask task) {
        this.task = task;
        doSchedule();
    }

    @Override
    protected void doSchedule() {
        String taskInstanceId = RegistryHelper.META_TASKS + SP + context.getAppName() + SP + context.getTaskName() + SP + context.getTaskId();
        if (!task.getRegistryHelper().exists(taskInstanceId)) {
            task.getRegistryHelper().create(taskInstanceId, context, CreateMode.PERSISTENT);
        }
        // TODO: 开启监听
    }

    @Override
    protected boolean isScheduled() {
        return scheduleStatus == ScheduleStatus.FINISHED;
    }

    public void setContext(TaskExecutionContext context) {
        this.context = context;
    }
}
