package rabbit.open.libra.client.dag;

/**
 * dag运行态实例
 * @author xiaoqianbin
 * @date 2020/8/20
 **/
public class RuntimeDagInstance extends SchedulableDirectedAcyclicGraph {

    // 调度id
    private String scheduleId;

    public RuntimeDagInstance(DistributedTaskNode head, DistributedTaskNode tail, int maxNodeSize) {
        super(head, tail, maxNodeSize);
    }

    public String getScheduleId() {
        return scheduleId;
    }

    public void setScheduleId(String scheduleId) {
        this.scheduleId = scheduleId;
    }
}
