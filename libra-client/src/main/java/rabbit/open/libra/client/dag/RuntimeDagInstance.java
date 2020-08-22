package rabbit.open.libra.client.dag;

/**
 * dag运行态实例
 * @author xiaoqianbin
 * @date 2020/8/20
 **/
@SuppressWarnings("serial")
public class RuntimeDagInstance extends SchedulableDirectedAcyclicGraph {

    // 调度id
    private String scheduleId;

    public RuntimeDagInstance(DagTaskNode head, DagTaskNode tail, int maxNodeSize) {
        super(head, tail, maxNodeSize);
    }

    public String getScheduleId() {
        return scheduleId;
    }

    public void setScheduleId(String scheduleId) {
        this.scheduleId = scheduleId;
    }
}
