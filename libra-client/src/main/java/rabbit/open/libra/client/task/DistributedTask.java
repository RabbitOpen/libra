package rabbit.open.libra.client.task;

import org.springframework.beans.factory.annotation.Autowired;
import rabbit.open.libra.client.Task;

import javax.annotation.PostConstruct;

/**
 * 分布式任务
 * @author xiaoqianbin
 * @date 2020/8/11
 **/
public abstract class DistributedTask implements Task {

    /**
     * 任务监听器
     * @author xiaoqianbin
     * @date 2020/8/14
     **/
    @Autowired
    protected TaskSubscriber monitor;

    @PostConstruct
    public void init() {
        monitor.register(this);
    }

    /**
     * 中断任务
     * @author xiaoqianbin
     * @date 2020/8/24
     **/
    public void interrupt() {
    }

    public void setMonitor(TaskSubscriber monitor) {
        this.monitor = monitor;
    }
}
