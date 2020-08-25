package rabbit.open.libra.test.tasks;

import rabbit.open.libra.client.dag.DagTaskNode;

/**
 * 任务节点样例
 * @author xiaoqianbin
 * @date 2020/8/18
 **/
@SuppressWarnings("serial")
public class SampleDagTaskNode extends DagTaskNode {

    public SampleDagTaskNode(String name) {
        super(name, 1, 1, "test-app");
    }

    @Override
    public String toString() {
        return taskName;
    }
}
