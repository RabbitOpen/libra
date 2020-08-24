package rabbit.open.libra.client.dag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rabbit.open.libra.client.task.SchedulerTask;
import rabbit.open.libra.dag.DirectedAcyclicGraph;

import java.util.Date;

/**
 * 可调度的有向无环图
 * @author xiaoqianbin
 * @date 2020/8/18
 **/
@SuppressWarnings("serial")
public class SchedulableDirectedAcyclicGraph extends DirectedAcyclicGraph<DagTaskNode> {

    protected Logger logger = LoggerFactory.getLogger(getClass());

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
		for (DagTaskNode node : nodes) {
			node.setTask(task);
			node.setGraph(this);
		}
	}
	
	/**
	 * <b>@description 注入dag </b>
	 */
	public void injectNodeGraph() {
		for (DagTaskNode node : nodes) {
			node.setGraph(this);
		}
	}

    /**
     * 调度完成
     * @author xiaoqianbin
     * @date 2020/8/18
     **/
    @Override
    protected void onScheduleFinished() {
        logger.info("dag is scheduled");
        task.scheduleFinished(getDagId());
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
