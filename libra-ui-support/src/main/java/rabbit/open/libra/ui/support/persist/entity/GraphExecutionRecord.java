package rabbit.open.libra.ui.support.persist.entity;

import rabbit.open.orm.core.annotation.Entity;

import java.util.Date;

/**
 * dag任务执行记录
 * @author xiaoqianbin
 * @date 2020/7/28
 **/
@Entity(value = "GRAPH_EXECUTION_RECORD", autoSpeculate = true)
public class GraphExecutionRecord extends ExecutionRecord {

    /**
     * 调度日期
     **/
    private Date scheduleDate;

    /**
     * 触发时间
     **/
    private Date fireDate;

    /**
     * dag id
     **/
    private String dagId;

    /**
     * schedule id
     **/
    private String scheduleId;

    /**
     * task node count
     **/
    private int taskNodeNum;

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

    public String getDagId() {
        return dagId;
    }

    public void setDagId(String dagId) {
        this.dagId = dagId;
    }

    public String getScheduleId() {
        return scheduleId;
    }

    public void setScheduleId(String scheduleId) {
        this.scheduleId = scheduleId;
    }

    public int getTaskNodeNum() {
        return taskNodeNum;
    }

    public void setTaskNodeNum(int taskNodeNum) {
        this.taskNodeNum = taskNodeNum;
    }
}
