package rabbit.open.libra.client.dag;

import rabbit.open.libra.client.meta.DagMeta;
import rabbit.open.libra.dag.DirectedAcyclicGraph;
import rabbit.open.libra.dag.schedule.ScheduleContext;

/**
 * 可调度的有向无环图
 * @author xiaoqianbin
 * @date 2020/8/18
 **/
public class SchedulableDag extends DirectedAcyclicGraph<DistributedTaskNode> {

    private ScheduleContext context;

    // dag 元信息
    private DagMeta dagMeta;

    public SchedulableDag(DistributedTaskNode head, DistributedTaskNode tail) {
        super(head, tail);
    }

    public SchedulableDag(DistributedTaskNode head, DistributedTaskNode tail, int maxNodeSize) {
        super(head, tail, maxNodeSize);
    }

    /**
     * 调度完成
     * @author xiaoqianbin
     * @date 2020/8/18
     **/
    @Override
    protected void onScheduleFinished() {

    }

    @Override
    protected ScheduleContext getContext() {
        return context;
    }

    public void setContext(ScheduleContext context) {
        this.context = context;
    }

    @Override
    protected void flushContext() {

    }

    public DagMeta getDagMeta() {
        return dagMeta;
    }

    public void setDagMeta(DagMeta dagMeta) {
        this.dagMeta = dagMeta;
    }

}
