package rabbit.open.libra.dag;

import rabbit.open.libra.dag.schedule.ScheduleContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * dag节点信息
 * @author xiaoqianbin
 * @date 2020/8/7
 **/
@SuppressWarnings("serial")
public abstract class DagNode implements Serializable {

    /**
     * 下一批节点
     **/
    protected List<DagNode> nextNodes = new ArrayList<>();

    /**
     * 前一批节点
     **/
    protected List<DagNode> preNodes = new ArrayList<>();

    /**
     * 执行状态
     **/
    protected ExecutionStatus status = ExecutionStatus.INIT;

    /**
     * 节点所属dag图
     **/
    protected DirectedAcyclicGraph<? extends DagNode> graph;

    /**
     * 添加后续节点
     * @param	next
     * @author  xiaoqianbin
     * @date    2020/8/7
     **/
    public void addNextNode(DagNode next) {
        this.nextNodes.add(next);
        next.preNodes.add(this);
    }

    /**
     * 执行调度
     * @param   context
     * @author  xiaoqianbin
     * @date    2020/8/8
     **/
    public abstract void doSchedule(ScheduleContext context);

    /**
     * 判断节点是否已经被调度过
     * @param	context
     * @author  xiaoqianbin
     * @date    2020/8/8
     **/
    protected boolean isScheduled(ScheduleContext context) {
        return ExecutionStatus.FINISHED == status;
    }

    public List<DagNode> getNextNodes() {
        return nextNodes;
    }

    public List<DagNode> getPreNodes() {
        return preNodes;
    }

    public void setStatus(ExecutionStatus status) {
        this.status = status;
    }

    public DirectedAcyclicGraph<? extends DagNode> getGraph() {
        return graph;
    }

    public void setGraph(DirectedAcyclicGraph<? extends DagNode> graph) {
        this.graph = graph;
    }

    public ExecutionStatus getStatus() {
        return status;
    }
}

