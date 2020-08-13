package rabbit.open.libra.client;

import org.springframework.scheduling.TriggerContext;
import org.springframework.scheduling.support.CronTrigger;

import java.io.Serializable;
import java.util.Date;

/**
 * task meta 信息
 * @author xiaoqianbin
 * @date 2020/7/11
 **/
@SuppressWarnings("serial")
public class TaskMeta implements Serializable {

    // 任务片
    private transient Task task;

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



    public TaskMeta(Task task) {
        setTask(task);
        setSplitsCount(task.getSplitsCount());
        setParallel(task.getConcurrenceCount());
        setTaskName(task.getTaskName());
    }
    
    /**
     * <b>@description 获取任务下次执行时间 </b>
     * @param lastCompletionTime
     * @param cronExpression
     * @return
     */
    public static Date getNextScheduleTime(Date lastCompletionTime, String cronExpression) {
    	CronTrigger trigger = new CronTrigger(cronExpression);
        return trigger.nextExecutionTime(new TriggerContext() {
            @Override
            public Date lastScheduledExecutionTime() {
                return null;
            }
            @Override
            public Date lastActualExecutionTime() {
                return null;
            }
            @Override
            public Date lastCompletionTime() {
                return lastCompletionTime;
            }
        });
    }


    public Task getTask() {
        return task;
    }

    public void setTask(Task task) {
        this.task = task;
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
}
