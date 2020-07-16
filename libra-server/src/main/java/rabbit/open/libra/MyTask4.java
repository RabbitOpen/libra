package rabbit.open.libra;

import org.springframework.stereotype.Component;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.task.DistributedTask;

import javax.annotation.Resource;

/**
 * @author xiaoqianbin
 * @date 2020/7/13
 **/
@Component
public class MyTask4 extends DistributedTask {

    @Resource
    RegistryHelper helper;

    @Override
    public RegistryHelper getRegistryHelper() {
        return helper;
    }

    @Override
    public void execute(int index, int splits, String taskScheduleTime) {
        logger.info("{}-->{}-{} is executed ", getTaskGroup(), getTaskName(), index);
    }

    @Override
    protected int getSplitsCount() {
        return 2;
    }

    @Override
    protected Integer getExecuteOrder() {
        return 10;
    }

    @Override
    protected String getTaskGroup() {
        return "G3";
    }

    @Override
    protected String getCronExpression() {
        return "0/10 * * * * *";
    }
}
