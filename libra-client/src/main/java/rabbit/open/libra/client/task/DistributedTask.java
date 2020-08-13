package rabbit.open.libra.client.task;

import org.springframework.beans.factory.annotation.Autowired;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.Task;
import rabbit.open.libra.client.TaskMeta;

import javax.annotation.PostConstruct;

/**
 * 分布式任务
 * @author xiaoqianbin
 * @date 2020/8/11
 **/
public abstract class DistributedTask extends Task {

    /**
     * zk交互对象
     **/
    @Autowired
    protected RegistryHelper helper;

    @PostConstruct
    public void init() {
        helper.registerTaskMeta(getAppName() + SP + getTaskName(), new TaskMeta(this), false);
    }

}
