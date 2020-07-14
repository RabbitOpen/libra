package rabbit.open.libra.client;

import java.io.Serializable;
import java.util.List;

/**
 * task meta 信息
 * @author xiaoqianbin
 * @date 2020/7/11
 **/
public class TaskMeta implements Serializable {

    // 任务片
    private transient TaskPiece taskPiece;

    /**
     * 任务执行顺序
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    private Integer executeOrder;

    /**
     * 切片个数
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    private int splitsCount;

    /**
     * 任务并行度
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    private int parallel;

    // 任务分组名
    private String groupName;

    // 任务名
    private String taskName;

    // 任务执行的时间表达式
    private List<String> crones;

    public TaskMeta(TaskPiece taskPiece) {
        setTaskPiece(taskPiece);
        setExecuteOrder(taskPiece.getExecuteOrder());
        setSplitsCount(taskPiece.getSplitsCount());
        setParallel(taskPiece.getParallel());
        setGroupName(taskPiece.getTaskGroup());
        setTaskName(taskPiece.getTaskName());
        setCrones(taskPiece.getCrones());
    }

    public TaskPiece getTaskPiece() {
        return taskPiece;
    }

    public void setTaskPiece(TaskPiece taskPiece) {
        this.taskPiece = taskPiece;
    }

    public Integer getExecuteOrder() {
        return executeOrder;
    }

    public void setExecuteOrder(Integer executeOrder) {
        this.executeOrder = executeOrder;
    }

    public int getSplitsCount() {
        return splitsCount;
    }

    public void setSplitsCount(int splitsCount) {
        this.splitsCount = splitsCount;
    }

    public int getParallel() {
        return parallel;
    }

    public void setParallel(int parallel) {
        this.parallel = parallel;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public List<String> getCrones() {
        return crones;
    }

    public void setCrones(List<String> crones) {
        this.crones = crones;
    }

    @Override
    public String toString() {
        return getTaskName();
    }
}
