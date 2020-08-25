package rabbit.open.libra.client.meta;

import rabbit.open.libra.dag.schedule.ScheduleContext;

import java.util.concurrent.Semaphore;

/**
 * task运行meta
 * @author xiaoqianbin
 * @date 2020/8/15
 **/
@SuppressWarnings("serial")
public class TaskExecutionContext extends ScheduleContext {

    private Semaphore semaphore;

    private String taskName;

    private String appName;

    public TaskExecutionContext(int parallel) {
        this.setParallel(parallel);
        this.semaphore = new Semaphore(getParallel());
    }

    /**
     * 判断任务是否还可以继续添加分片
     * @author xiaoqianbin
     * @date 2020/8/15
     **/
    public boolean hasQuota() {
        return semaphore.availablePermits() > 0;
    }

    /**
     * 抢占额度
     * @author xiaoqianbin
     * @date 2020/8/15
     **/
    public boolean grabQuota() {
        return semaphore.tryAcquire();
    }

    /**
     * 恢复额度
     * @author xiaoqianbin
     * @date 2020/8/15
     **/
    public void resume() {
        semaphore.release();
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }
}
