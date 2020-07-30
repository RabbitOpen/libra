package rabbit.open.libra.client.ui.support.persist.entity;

import rabbit.open.orm.common.dml.Policy;
import rabbit.open.orm.core.annotation.Column;
import rabbit.open.orm.core.annotation.Entity;
import rabbit.open.orm.core.annotation.PrimaryKey;

import java.util.Date;

/**
 * 任务执行记录
 * @author xiaoqianbin
 * @date 2020/7/28
 **/
@Entity(value = "TASK_EXECUTION_RECORD", autoSpeculate = true)
public class TaskExecutionRecord {

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

    private String appName;

    private String groupName;

    private String taskName;

    private String scheduleTime;

    public String getId() {
        return id;
    }

    public Date getStart() {
        return start;
    }

    public Date getEnd() {
        return end;
    }

    public String getAppName() {
        return appName;
    }

    public String getGroupName() {
        return groupName;
    }

    public String getTaskName() {
        return taskName;
    }

    public String getScheduleTime() {
        return scheduleTime;
    }

    public void setStart(Date start) {
        this.start = start;
    }

    public void setEnd(Date end) {
        this.end = end;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public void setScheduleTime(String scheduleTime) {
        this.scheduleTime = scheduleTime;
    }
}
