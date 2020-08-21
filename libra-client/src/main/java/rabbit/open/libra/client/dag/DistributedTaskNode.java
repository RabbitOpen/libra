package rabbit.open.libra.client.dag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rabbit.open.libra.client.task.SchedulerTask;
import rabbit.open.libra.dag.DagNode;
import rabbit.open.libra.dag.ScheduleStatus;

/**
 * 分布式任务节点
 * @author xiaoqianbin
 * @date 2020/8/18
 **/
public class DistributedTaskNode extends DagNode {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected SchedulerTask task;

    //
    public void doSchedule(SchedulerTask task) {
        this.task = task;
        doSchedule();
    }

    @Override
    public void doSchedule() {

        logger.info("create schedule task id ------------------");

    }

    @Override
    protected boolean isScheduled() {
        return scheduleStatus == ScheduleStatus.FINISHED;
    }
}
