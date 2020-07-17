package rabbit.open.test;

import org.springframework.stereotype.Component;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.task.DistributedTask;

import javax.annotation.Resource;

/**
 * @author xiaoqianbin
 * @date 2020/7/13
 **/
@Component
public class MyTask extends DistributedTask {

    @Resource
    RegistryHelper helper;

    @Override
    public RegistryHelper getRegistryHelper() {
        return helper;
    }

    @Override
    public void execute(int index, int splits, String taskScheduleTime) {
        logger.info("{}-->{}-{} is executed at {} ", getTaskGroup(), getTaskName(), index, taskScheduleTime);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected int getSplitsCount() {
        return 5;
    }

    @Override
    protected int getParallel() {
        return 2;
    }

    @Override
    protected Integer getExecuteOrder() {
        return 0;
    }

    @Override
    protected String getCronExpression() {
        return "0/5 * * * * *";
    }
}
