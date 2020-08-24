package rabbit.open.libra.client.dag;

import java.util.Date;

/**
 * dag运行态实例
 * @author xiaoqianbin
 * @date 2020/8/20
 **/
@SuppressWarnings("serial")
public class RuntimeDagInstance extends SchedulableDirectedAcyclicGraph {

    // 调度id
    private String scheduleId;
    
    /**
     * 业务调度日期
     */
    private Date scheduleDate;

    private boolean scheduled = false;

    /**
     * 实际调度时间
     */
    private Date fireDate;

    @Override
    public void startSchedule() {
        this.scheduled = true;
        super.startSchedule();
    }

    public boolean isScheduled() {
        return scheduled;
    }

    public RuntimeDagInstance(DagTaskNode head, DagTaskNode tail, int maxNodeSize) {
        super(head, tail, maxNodeSize);
    }

    public String getScheduleId() {
        return scheduleId;
    }

    public void setScheduleId(String scheduleId) {
        this.scheduleId = scheduleId;
    }

	public Date getScheduleDate() {
		return scheduleDate;
	}

	public void setScheduleDate(Date scheduleDate) {
		this.scheduleDate = scheduleDate;
	}

	public Date getFireDate() {
		return fireDate;
	}

	public void setFireDate(Date fireDate) {
		this.fireDate = fireDate;
	}
    
}
