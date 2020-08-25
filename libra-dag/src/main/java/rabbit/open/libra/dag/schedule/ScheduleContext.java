package rabbit.open.libra.dag.schedule;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 调度上下文
 * @author xiaoqianbin
 * @date 2020/8/8
 **/
@SuppressWarnings("serial")
public class ScheduleContext implements Serializable {

    public static final long serialVersionUID = 1L;

    private Map<String, Serializable> context = new HashMap<>();

    /**
     * 调度批次号
     **/
    private String scheduleId;

    /**
     * 任务id
     **/
    private String taskId;

    /**
     * 触发日期（实际日期）
     **/
    private Date fireDate;

    /**
     * 调度日期
     **/
    private Date scheduleDate;

    /**
     * 并发度
     **/
    private int parallel = 0;

    /**
     * 分片索引
     **/
    private int index = 0;

    /**
     * 切片数
     **/
    private int splitsCount = 0;

    public Serializable getContextValue(String key) {
        return context.get(key);
    }

    public Map<String, Serializable> getContext() {
        return context;
    }

    public Date getFireDate() {
        return fireDate;
    }

    public void setFireDate(Date fireDate) {
        this.fireDate = fireDate;
    }

    public Date getScheduleDate() {
        return scheduleDate;
    }

    public void setScheduleDate(Date scheduleDate) {
        this.scheduleDate = scheduleDate;
    }

    public String getScheduleId() {
        return scheduleId;
    }

    public void setContext(Map<String, Serializable> context) {
        this.context = context;
    }

    public void setScheduleId(String scheduleId) {
        this.scheduleId = scheduleId;
    }

    public int getParallel() {
        return parallel;
    }

    public void setParallel(int parallel) {
        this.parallel = parallel;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getSplitsCount() {
        return splitsCount;
    }

    public void setSplitsCount(int splitsCount) {
        this.splitsCount = splitsCount;
    }
}
