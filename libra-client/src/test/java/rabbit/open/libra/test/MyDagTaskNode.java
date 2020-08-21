package rabbit.open.libra.test;

import rabbit.open.libra.client.dag.DagTaskNode;

/**
 * @author xiaoqianbin
 * @date 2020/8/18
 **/
public class MyDagTaskNode extends DagTaskNode {

    private String name;

    public MyDagTaskNode(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
