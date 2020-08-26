package rabbit.open.libra.ui.support.persist.entity;

import rabbit.open.libra.dag.ScheduleStatus;
import rabbit.open.orm.common.dml.Policy;
import rabbit.open.orm.core.annotation.Column;
import rabbit.open.orm.core.annotation.PrimaryKey;

import java.util.Date;

/**
 * 执行记录基类
 * @author xiaoqianbin
 * @date 2020/8/26
 **/
public class ExecutionRecord {

    @PrimaryKey(policy = Policy.UUID)
    @Column(value = "id", length = 40)
    private String id;

    /**
     * 启动日期
     **/
    @Column(value = "start", keyWord = true)
    private Date start;

    /**
     * 结束日期
     **/
    @Column(value = "end", keyWord = true)
    private Date end;

    /**
     * 调度状态
     **/
    @Column(value = "schedule_status", length = 16)
    private ScheduleStatus status = ScheduleStatus.RUNNING;

    /**
     * 调度日期
     **/
    private Date scheduleDate;

    /**
     * 触发时间
     **/
    private Date fireDate;

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

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Date getStart() {
        return start;
    }

    public void setStart(Date start) {
        this.start = start;
    }

    public Date getEnd() {
        return end;
    }

    public void setEnd(Date end) {
        this.end = end;
    }

    public ScheduleStatus getStatus() {
        return status;
    }

    public void setStatus(ScheduleStatus status) {
        this.status = status;
    }
}
