package rabbit.open.libra.test.tasks;

import org.springframework.stereotype.Component;
import rabbit.open.libra.client.task.DistributedTask;
import rabbit.open.libra.dag.schedule.ScheduleContext;

import java.util.function.Consumer;

/**
 * @author xiaoqianbin
 * @date 2020/8/14
 **/
@Component
public class SimpleTask extends DistributedTask {

    private Consumer<ScheduleContext> task;

    @Override
    public void execute(ScheduleContext context) {
        if (null != task) {
            task.accept(context);
        }
    }

    public void setTask(Consumer<ScheduleContext> task) {
        this.task = task;
    }

    public SimpleTask() {
        super();
    }


}
