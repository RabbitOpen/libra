package rabbit.open.libra.dag;

import rabbit.open.libra.dag.exception.DagException;

import java.util.List;

/**
 * dag图对象
 * @author xiaoqianbin
 * @date 2020/8/7
 **/
public class DirectedAcyclicGraph<T extends DagNode> {

    private T head;

    public DirectedAcyclicGraph(T head) {
        this.head = head;
        List<DagNode> tails = head.tails();
        if (tails.size() > 1) {
            throw new DagException("no end node exception");
        }
    }

}
