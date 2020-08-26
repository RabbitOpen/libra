package rabbit.open.libra.client.dag;

import org.springframework.scheduling.TriggerContext;
import org.springframework.scheduling.support.CronTrigger;
import rabbit.open.libra.client.task.SchedulerTask;
import rabbit.open.libra.dag.DirectedAcyclicGraph;

import java.util.Date;

/**
 * 可调度的有向无环图
 * @author xiaoqianbin
 * @date 2020/8/18
 **/
public class SchedulableDirectedAcyclicGraph extends DirectedAcyclicGraph<DagTaskNode> {

    private static final long serialVersionUID = 1L;

    // dag name
    private String dagName;

    // dag id信息
    private String dagId;

    // 调度周期表达式
    private String cronExpression;

    // 上一个调度日期 yyyy-MM-dd HH:mm:ss
    private Date lastFireDate;

    private transient SchedulerTask task;

    public SchedulableDirectedAcyclicGraph(DagTaskNode head, DagTaskNode tail) {
        super(head, tail);
    }

    public SchedulableDirectedAcyclicGraph(DagTaskNode head, DagTaskNode tail, int maxNodeSize) {
        super(head, tail, maxNodeSize);
    }

    /**
     * <b>@description 注入task对象 </b>
     * @param task
     */
    public void injectTask(SchedulerTask task) {
        for (DagTaskNode node : getNodes()) {
            node.setTask(task);
            node.setGraph(this);
        }
    }

    /**
     * <b>@description 注入dag </b>
     */
    public void injectNodeGraph() {
        for (DagTaskNode node : getNodes()) {
            node.setGraph(this);
        }
    }

    /**
     * <b>@description 获取任务下次执行时间 </b>
     * @return
     */
    public Date getNextScheduleTime() {
        CronTrigger trigger = new CronTrigger(getCronExpression());
        return trigger.nextExecutionTime(new TriggerContext() {

            @Override
            public Date lastScheduledExecutionTime() {
                return null;
            }

            @Override
            public Date lastActualExecutionTime() {
                return null;
            }

            @Override
            public Date lastCompletionTime() {
                return getLastFireDate();
            }
        });
    }

    /**
     * 调度完成
     * @author xiaoqianbin
     * @date 2020/8/18
     **/
    @Override
    protected void onScheduleFinished() {
        task.scheduleFinished((RuntimeDagInstance) this);
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

}
