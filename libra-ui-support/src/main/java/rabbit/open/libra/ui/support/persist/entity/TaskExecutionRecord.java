package rabbit.open.libra.ui.support.persist.entity;

import rabbit.open.orm.core.annotation.Entity;

/**
 * 任务执行记录
 * @author xiaoqianbin
 * @date 2020/7/28
 **/
@Entity(value = "TASK_EXECUTION_RECORD", autoSpeculate = true)
public class TaskExecutionRecord extends ExecutionRecord {

    /**
     * app name
     **/
    private String appName;

    /**
     * 任务名
     **/
    private String taskName;

    /**
     * 任务id
     **/
    private String taskId;

    /**
     * 调度id
     **/
    private String scheduleId;

    /**
     * 切片数
     **/
    private Integer splitsCount;

    /**
     * 成功切片数
     **/
    private Integer success;

    /**
     * 失败切片数
     **/
    private Integer failed;

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getScheduleId() {
        return scheduleId;
    }

    public void setScheduleId(String scheduleId) {
        this.scheduleId = scheduleId;
    }

    public Integer getSplitsCount() {
        return splitsCount;
    }

    public void setSplitsCount(Integer splitsCount) {
        this.splitsCount = splitsCount;
    }

    public Integer getSuccess() {
        return success;
    }

    public void setSuccess(Integer success) {
        this.success = success;
    }

    public Integer getFailed() {
        return failed;
    }

    public void setFailed(Integer failed) {
        this.failed = failed;
    }
}
