package rabbit.open.libra.test;

import rabbit.open.libra.client.dag.DistributedTaskNode;

/**
 * @author xiaoqianbin
 * @date 2020/8/18
 **/
public class MyDistributedTaskNode extends DistributedTaskNode {

    private String name;

    public MyDistributedTaskNode(String name) {
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
