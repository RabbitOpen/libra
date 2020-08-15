package rabbit.open.libra.client.meta;

import rabbit.open.libra.dag.schedule.ScheduleContext;

import java.io.Serializable;
import java.util.concurrent.Semaphore;

/**
 * task运行meta
 * @author xiaoqianbin
 * @date 2020/8/15
 **/
@SuppressWarnings("serial")
public class TaskExecutionMeta extends ScheduleContext implements Serializable {

    private Semaphore semaphore;

    public TaskExecutionMeta(int parallel) {
        this.setParallel(parallel);
        this.semaphore = new Semaphore(getParallel());
    }

    /**
     * 判断任务是否还可以继续添加分片
     * @author  xiaoqianbin
     * @date    2020/8/15
     **/
    public boolean hasQuota() {
        return semaphore.availablePermits() > 0;
    }

    /**
     * 抢占额度
     * @author  xiaoqianbin
     * @date    2020/8/15
     **/
    public boolean grabQuota() {
        return semaphore.tryAcquire();
    }

    /**
     * 恢复额度
     * @author  xiaoqianbin
     * @date    2020/8/15
     **/
    public void resume() {
        semaphore.release();
    }

}
