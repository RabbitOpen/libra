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

    Map<String, Serializable> context = new HashMap<>();

    /**
     * 调度批次号
     **/
    private String scheduleId;

    /**
     * 触发日期（实际日期）
     **/
    private Date fireDate;

    /**
     * 调度日期
     **/
    private Date scheduleDate;

    public void setContext(String key, Serializable value) {
        this.context.put(key, value);
    }

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
}
