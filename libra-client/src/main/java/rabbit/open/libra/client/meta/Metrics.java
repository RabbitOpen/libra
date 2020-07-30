package rabbit.open.libra.client.meta;

import rabbit.open.libra.client.ScheduleType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 任务切片执行信息
 * @author xiaoqianbin
 * @date 2020/7/29
 **/
@SuppressWarnings("serial")
public class Metrics implements Serializable {

    private String taskId;

    // 分片执行状态
    private Map<Integer, ExecutionMeta> metaMap = new HashMap<>();

    // 调度类型
    private ScheduleType scheduleType;

    public Metrics(String taskId, ScheduleType scheduleType) {
        this.taskId = taskId;
        this.scheduleType = scheduleType;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public Map<Integer, ExecutionMeta> getMetaMap() {
        return metaMap;
    }

    public void setMetaMap(Map<Integer, ExecutionMeta> metaMap) {
        this.metaMap = metaMap;
    }

    public ScheduleType getScheduleType() {
        return scheduleType;
    }

    public void setScheduleType(ScheduleType scheduleType) {
        this.scheduleType = scheduleType;
    }
}
