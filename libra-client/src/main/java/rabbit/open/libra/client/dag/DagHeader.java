package rabbit.open.libra.client.dag;

/**
 * dag 开始节点
 * @author xiaoqianbin
 * @date 2020/8/21
 **/
@SuppressWarnings("serial")
public class DagHeader extends DagTaskNode {

    @Override
    protected void doSchedule() {
        getGraph().onDagNodeExecuted(this);
    }
}
