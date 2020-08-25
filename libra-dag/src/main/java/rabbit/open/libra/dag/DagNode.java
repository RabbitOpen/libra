package rabbit.open.libra.dag;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * dag节点信息
 * @author xiaoqianbin
 * @date 2020/8/7
 **/
@SuppressWarnings({"serial", "unchecked"})
public abstract class DagNode implements Serializable {

    /**
     * 下一批节点
     **/
    private List<DagNode> nextNodes = new ArrayList<>();

    /**
     * 前一批节点
     **/
    private List<DagNode> preNodes = new ArrayList<>();

    /**
     * 调度状态
     **/
    protected ScheduleStatus scheduleStatus = ScheduleStatus.INIT;

    /**
     * 节点所属dag图
     **/
    protected transient DirectedAcyclicGraph<? extends DagNode> graph;

    /**
     * 添加后续节点
     * @param    next
     * @author xiaoqianbin
     * @date 2020/8/7
     **/
    public void addNextNode(DagNode next) {
        this.nextNodes.add(next);
        next.preNodes.add(this);
    }

    /**
     * 执行调度
     * @author xiaoqianbin
     * @date 2020/8/8
     **/
    protected abstract void doSchedule();

    /**
     * 判断节点是否已经被调度过
     * @author xiaoqianbin
     * @date 2020/8/8
     **/
    protected abstract boolean isScheduled();

    public List<DagNode> getNextNodes() {
        return nextNodes;
    }

    public List<DagNode> getPreNodes() {
        return preNodes;
    }

    public void setScheduleStatus(ScheduleStatus scheduleStatus) {
        this.scheduleStatus = scheduleStatus;
    }

    public <T extends DagNode, D extends DirectedAcyclicGraph<T>> D getGraph() {
        return (D) graph;
    }

    public void setGraph(DirectedAcyclicGraph<? extends DagNode> graph) {
        this.graph = graph;
    }

    public ScheduleStatus getScheduleStatus() {
        return scheduleStatus;
    }
}

