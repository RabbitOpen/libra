package rabbit.open.libra.client.dag;

/**
 * dag 开始节点
 * @author xiaoqianbin
 * @date 2020/8/21
 **/
@SuppressWarnings("serial")
public class DagHeader extends DagTaskNode {

    public DagHeader() {
        this("header", 1, 1, "no-app");
    }

    public DagHeader(String taskName, int parallel, int splitsCount, String appName) {
        super(taskName, parallel, splitsCount, appName);
    }

    @Override
    protected void doSchedule() {
        getGraph().onDagNodeExecuted(this);
    }
}
