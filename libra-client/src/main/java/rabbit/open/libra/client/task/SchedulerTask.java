package rabbit.open.libra.client.task;

import org.springframework.beans.factory.annotation.Autowired;
import rabbit.open.libra.client.AbstractLibraTask;
import rabbit.open.libra.client.ExecuteType;
import rabbit.open.libra.client.RegistryHelper;

/**
 * 调度任务，分布式任务的调度核心，负责调度所有其他任务
 * @author xiaoqianbin
 * @date 2020/7/11
 **/
public class SchedulerTask extends AbstractLibraTask {

    public final static String GROUP_NAME = "SYSTEM";

    @Autowired
    private RegistryHelper helper;

    @Override
    public RegistryHelper getRegistryHelper() {
        return helper;
    }

    @Override
    public void execute(int index, int splits) {

    }

    @Override
    protected ExecuteType getExecuteType() {
        return ExecuteType.ACTIVE;
    }

    @Override
    protected String getTaskGroup() {
        return GROUP_NAME;
    }
}
