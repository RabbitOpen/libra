package rabbit.open.libra.test.tasks;

import java.util.function.Consumer;

import org.springframework.stereotype.Component;
import rabbit.open.libra.client.task.DistributedTask;
import rabbit.open.libra.dag.schedule.ScheduleContext;

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

	public SimpleTask(Consumer<ScheduleContext> task) {
		super();
		this.task = task;
	}

	public SimpleTask() {
		super();
	}
    
    
}
