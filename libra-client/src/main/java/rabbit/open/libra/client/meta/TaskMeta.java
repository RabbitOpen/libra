package rabbit.open.libra.client.meta;

import rabbit.open.libra.client.Task;

import java.io.Serializable;

/**
 * task meta 信息
 * @author xiaoqianbin
 * @date 2020/7/11
 **/
@SuppressWarnings("serial")
public class TaskMeta implements Serializable {

    /**
     * 切片个数
     * @author xiaoqianbin
     * @date 2020/7/13
     **/
    private int splitsCount;

    /**
     * 任务并行度
     * @author xiaoqianbin
     * @date 2020/7/13
     **/
    private int parallel;

    // 任务名
    private String taskName;

    public TaskMeta(Task task) {
        setSplitsCount(task.getSplitsCount());
        setParallel(task.getParallel());
        setTaskName(task.getTaskName());
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

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }
}
