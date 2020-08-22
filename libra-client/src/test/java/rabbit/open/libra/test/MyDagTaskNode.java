package rabbit.open.libra.test;

import rabbit.open.libra.client.dag.DagTaskNode;

/**
 * @author xiaoqianbin
 * @date 2020/8/18
 **/
@SuppressWarnings("serial")
public class MyDagTaskNode extends DagTaskNode {

    public MyDagTaskNode(String name) {
        super(name, 1, 1, "test-app");
    }

    @Override
    public String toString() {
        return taskName;
    }
}
