package rabbit.open.libra.dag;

import rabbit.open.libra.dag.exception.CyclicDagException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * dag节点信息
 * @author xiaoqianbin
 * @date 2020/8/7
 **/
public class DagNode {

    /**
     * 下一批节点
     **/
    protected Set<DagNode> nextNodes = new HashSet<>();

    /**
     * 前一批节点
     **/
    protected Set<DagNode> preNodes = new HashSet<>();

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
     * 获取从当前节点开始的最末端节点集合
     * @author  xiaoqianbin
     * @date    2020/8/8
     **/
    public List<DagNode> tails() {
        return tails(new HashSet<>());
    }

    /**
     * 获取尾部节点
     * @param	cyclicCheckingSet
     * @author  xiaoqianbin
     * @date    2020/8/8
     **/
    private List<DagNode> tails(Set<DagNode> cyclicCheckingSet) {
        doDagCyclicChecking(cyclicCheckingSet);
        List<DagNode> tailList = new ArrayList<>();
        if (nextNodes.isEmpty()) {
            tailList.add(this);
            return tailList;
        } else {
            for (DagNode nextNode : nextNodes) {
                tailList.addAll(nextNode.tails(cyclicCheckingSet));
            }
            return tailList;
        }
    }

    /**
     * 判断是否存在环
     * @author  xiaoqianbin
     * @date    2020/8/8
     **/
    public boolean circleExisted() {
        try {
            tails(new HashSet<>());
            return false;
        } catch (CyclicDagException e) {
            return true;
        }
    }

    /**
     * 检测dag循环异常
     * @param	cyclicCheckingSet
     * @author  xiaoqianbin
     * @date    2020/8/8
     **/
    protected void doDagCyclicChecking(Set<DagNode> cyclicCheckingSet) {
        if (!cyclicCheckingSet.contains(this)) {
            cyclicCheckingSet.add(this);
        } else {
            throw new CyclicDagException("dag cyclic exception");
        }
    }

    public Set<DagNode> getNextNodes() {
        return nextNodes;
    }

    public Set<DagNode> getPreNodes() {
        return preNodes;
    }
}

