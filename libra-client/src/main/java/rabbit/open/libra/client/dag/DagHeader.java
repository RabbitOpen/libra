package rabbit.open.libra.client.dag;

/**
 * dag 开始节点
 * @author xiaoqianbin
 * @date 2020/8/21
 **/
public class DagHeader extends DagTaskNode {

    @Override
    public void doSchedule() {
        graph.onDagNodeExecuted(this);
    }
}
