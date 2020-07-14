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
public class MyTask2 extends DistributedTask {

    @Resource
    RegistryHelper helper;

    @Override
    public RegistryHelper getRegistryHelper() {
        return helper;
    }

    @Override
    public void execute(int index, int splits, String taskScheduleTime) {

    }

    @Override
    protected void close() {

    }

    @Override
    protected Integer getExecuteOrder() {
        return 1;
    }
}
