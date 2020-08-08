package rabbit.open.libra.dag;

import rabbit.open.libra.dag.exception.DagException;

import java.util.ArrayList;
import java.util.List;

/**
 * dag图对象
 * @author xiaoqianbin
 * @date 2020/8/7
 **/
public class DirectedAcyclicGraph<T extends DagNode> {

    private T head;

    private T tail;

    public DirectedAcyclicGraph(T head, T tail) {
        this.head = head;
        this.tail = tail;
        List<T> nodes = new ArrayList<>();
        nodes.add(head);
        for (DagNode nextNode : head.getNextNodes()) {
            if (nodes.contains(nextNode)) {
                throw new DagException("no end node exception");
            }
            if ( nextNode.nextNodes.size() == 1 && nextNode.getNextNodes().iterator().next() == tail) {

            }
        }

    }

}
