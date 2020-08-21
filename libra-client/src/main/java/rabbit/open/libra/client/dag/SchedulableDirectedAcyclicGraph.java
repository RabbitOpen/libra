package rabbit.open.libra.client.dag;

import rabbit.open.libra.client.task.SchedulerTask;
import rabbit.open.libra.dag.DirectedAcyclicGraph;

import java.util.Date;

/**
 * 可调度的有向无环图
 * @author xiaoqianbin
 * @date 2020/8/18
 **/
public class SchedulableDirectedAcyclicGraph extends DirectedAcyclicGraph<DistributedTaskNode> implements VersionedData {

    // dag name
    private String dagName;

    // dag id信息
    private String dagId;

    // 调度周期表达式
    private String cronExpression;

    // 上一个调度日期 yyyy-MM-dd HH:mm:ss
    private Date lastFireDate;

    private transient SchedulerTask task;

    /**
     * 数据版本号
     **/
    private int version = 0;

    public SchedulableDirectedAcyclicGraph(DistributedTaskNode head, DistributedTaskNode tail) {
        super(head, tail);
    }

    public SchedulableDirectedAcyclicGraph(DistributedTaskNode head, DistributedTaskNode tail, int maxNodeSize) {
        super(head, tail, maxNodeSize);
    }

    /**
     * 调度完成
     * @author xiaoqianbin
     * @date 2020/8/18
     **/
    @Override
    protected void onScheduleFinished() {

    }

    @Override
    protected void saveGraph() {
        task.saveRuntimeGraph(this);
    }

    public void setTask(SchedulerTask task) {
        this.task = task;
    }

    public String getDagName() {
        return dagName;
    }

    public void setDagName(String dagName) {
        this.dagName = dagName;
    }

    public String getDagId() {
        return dagId;
    }

    public void setDagId(String dagId) {
        this.dagId = dagId;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    public Date getLastFireDate() {
        return lastFireDate;
    }

    public void setLastFireDate(Date lastFireDate) {
        this.lastFireDate = lastFireDate;
    }

    @Override
    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}
