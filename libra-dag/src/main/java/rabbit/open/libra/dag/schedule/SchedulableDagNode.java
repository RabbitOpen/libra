package rabbit.open.libra.dag.schedule;

import rabbit.open.libra.dag.DagNode;

/**
 * 可调度的dag节点
 * @author xiaoqianbin
 * @date 2020/8/8
 **/
public abstract class SchedulableDagNode extends DagNode {

    /**
     * 执行调度
     * @param   context
     * @author  xiaoqianbin
     * @date    2020/8/8
     **/
    public void doSchedule(ScheduleContext context) {
        if (getPreNodes().isEmpty()) {
            schedule(context);
            getNextNodes().forEach(node -> ((SchedulableDagNode)node).doSchedule(context));
        } else {

        }
    }

    public abstract void schedule(ScheduleContext context);

}

