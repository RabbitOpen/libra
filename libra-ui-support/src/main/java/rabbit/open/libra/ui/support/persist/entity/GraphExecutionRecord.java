package rabbit.open.libra.ui.support.persist.entity;

import rabbit.open.orm.core.annotation.Entity;

/**
 * dag任务执行记录
 * @author xiaoqianbin
 * @date 2020/7/28
 **/
@Entity(value = "GRAPH_EXECUTION_RECORD", autoSpeculate = true)
public class GraphExecutionRecord extends ExecutionRecord {

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
    private Integer taskNodeNum;

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

    public void setTaskNodeNum(Integer taskNodeNum) {
        this.taskNodeNum = taskNodeNum;
    }
}
