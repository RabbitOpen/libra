package rabbit.open.libra.client.dag;

import rabbit.open.libra.dag.DagNode;
import rabbit.open.libra.dag.schedule.ScheduleContext;

/**
 * 分布式任务节点
 * @author xiaoqianbin
 * @date 2020/8/18
 **/
public class DistributedTaskNode extends DagNode {

    @Override
    public void doSchedule(ScheduleContext context) {

        // TODO: CREATE TASK NODE

    }

    @Override
    protected boolean isScheduled(ScheduleContext context) {
        return false;
    }
}
