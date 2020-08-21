package rabbit.open.libra.client.dag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rabbit.open.libra.client.meta.TaskExecutionContext;
import rabbit.open.libra.client.task.SchedulerTask;
import rabbit.open.libra.dag.DagNode;
import rabbit.open.libra.dag.ScheduleStatus;

/**
 * dag任务节点
 * @author xiaoqianbin
 * @date 2020/8/18
 **/
public class DagTaskNode extends DagNode {

    protected transient Logger logger = LoggerFactory.getLogger(getClass());

    protected transient SchedulerTask task;

    private TaskExecutionContext executionContext;

    // 执行调度
    public void doSchedule(SchedulerTask task) {
        this.task = task;
        doSchedule();
    }

    @Override
    protected void doSchedule() {

    }

    @Override
    protected boolean isScheduled() {
        return scheduleStatus == ScheduleStatus.FINISHED;
    }
}
