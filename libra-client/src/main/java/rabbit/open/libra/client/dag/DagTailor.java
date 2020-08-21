package rabbit.open.libra.client.dag;

/**
 * dag 结束节点
 * @author xiaoqianbin
 * @date 2020/8/21
 **/
public class DagTailor extends DagTaskNode {


    @Override
    public void doSchedule() {
        graph.onDagNodeExecuted(this);
    }
}
