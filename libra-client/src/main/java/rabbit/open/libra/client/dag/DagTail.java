package rabbit.open.libra.client.dag;

/**
 * dag 结束节点
 * @author xiaoqianbin
 * @date 2020/8/21
 **/
@SuppressWarnings("serial")
public class DagTail extends DagTaskNode {

    public DagTail() {
        this("tail", 1, 1, "no-app");
    }

    public DagTail(String taskName, int parallel, int splitsCount, String appName) {
        super(taskName, parallel, splitsCount, appName);
    }

    @Override
    protected void doSchedule() {
        getGraph().onDagNodeExecuted(this);
    }
}
