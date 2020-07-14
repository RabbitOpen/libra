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
public class MyTask extends DistributedTask {

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
        return 0;
    }

    @Override
    protected List<String> getCrones() {
        List<String> list = new ArrayList<>();
        list.add("0/30 * * * * *");
        return list;
    }
}
