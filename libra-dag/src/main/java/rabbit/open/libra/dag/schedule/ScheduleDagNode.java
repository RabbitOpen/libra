package rabbit.open.libra.dag.schedule;

import rabbit.open.libra.dag.DagNode;

/**
 * 可调度的dag节点
 * @author xiaoqianbin
 * @date 2020/8/8
 **/
public abstract class ScheduleDagNode extends DagNode {

    /**
     * 执行调度
     * @param   context
     * @author  xiaoqianbin
     * @date    2020/8/8
     **/
    public void schedule(ScheduleContext context) {
        if (getPreNodes().isEmpty()) {
            doScheduledJob(context);
        } else {
            if (canSchedule(context)) {
                doScheduledJob(context);
            }
        }
    }

    /**
     * 判断是否所有前序节点都已经完成了
     * @param	context
     * @author  xiaoqianbin
     * @date    2020/8/8
     **/
    private boolean canSchedule(ScheduleContext context) {
        for (DagNode preNode : getPreNodes()) {
            if (!((ScheduleDagNode)preNode).isScheduled(context)) {
                return false;
            }
        }
        return true;
    }

    public abstract void doScheduledJob(ScheduleContext context);

    /**
     * 判断节点是否已经被调度过
     * @param	context
     * @author  xiaoqianbin
     * @date    2020/8/8
     **/
    protected abstract boolean isScheduled(ScheduleContext context);
}

