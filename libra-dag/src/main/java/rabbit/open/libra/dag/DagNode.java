package rabbit.open.libra.dag;

import java.util.ArrayList;
import java.util.List;

/**
 * dag节点信息
 * @author xiaoqianbin
 * @date 2020/8/7
 **/
public class DagNode {

    /**
     * 下一批节点
     **/
    protected List<DagNode> nextNodes = new ArrayList<>();

    /**
     * 前一批节点
     **/
    protected List<DagNode> preNodes = new ArrayList<>();

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
        List<DagNode> tailList = new ArrayList<>();
        if (nextNodes.isEmpty()) {
            tailList.add(this);
            return tailList;
        } else {
            for (DagNode nextNode : nextNodes) {
                tailList.addAll(nextNode.tails());
            }
            return tailList;
        }
    }

    public List<DagNode> getNextNodes() {
        return nextNodes;
    }

    public List<DagNode> getPreNodes() {
        return preNodes;
    }
}

