package rabbit.open.libra.client.task;

import org.springframework.beans.factory.annotation.Autowired;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.Task;
import rabbit.open.libra.client.TaskSubscriber;
import rabbit.open.libra.dag.schedule.ScheduleContext;

import javax.annotation.PostConstruct;

/**
 * 分布式任务
 * @author xiaoqianbin
 * @date 2020/8/11
 **/
public abstract class DistributedTask extends Task {

    /**
     * 任务监听器
     * @author  xiaoqianbin
     * @date    2020/8/14
     **/
    @Autowired
    protected TaskSubscriber monitor;

    /**
     * zk交互对象
     **/
    @Autowired
    protected RegistryHelper helper;

    @PostConstruct
    public void init() {
        monitor.register(this);
    }

    @Override
    public void execute(ScheduleContext context) {

    }
}
