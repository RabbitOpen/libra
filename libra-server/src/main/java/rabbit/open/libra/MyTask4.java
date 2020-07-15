package rabbit.open.libra;

import org.springframework.stereotype.Component;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.task.DistributedTask;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

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

    }

    @Override
    protected void close() {

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
        return "0/30 * * * * *";
    }
}
