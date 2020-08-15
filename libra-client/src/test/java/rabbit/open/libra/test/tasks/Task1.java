package rabbit.open.libra.test.tasks;

import org.springframework.stereotype.Component;
import rabbit.open.libra.client.task.DistributedTask;
import rabbit.open.libra.dag.schedule.ScheduleContext;

/**
 * @author xiaoqianbin
 * @date 2020/8/14
 **/
@Component
public class Task1 extends DistributedTask {

    @Override
    public void execute(ScheduleContext context) {

    }
}
