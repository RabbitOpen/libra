package rabbit.open.libra.ui.test;

import org.springframework.stereotype.Component;
import rabbit.open.libra.client.dag.RuntimeDagInstance;
import rabbit.open.libra.ui.support.task.WebSupportedSchedulerTask;

import java.util.function.Consumer;

/**
 * @author xiaoqianbin
 * @date 2020/8/26
 **/
@Component
public class MyWebSupportedSchedulerTask extends WebSupportedSchedulerTask {

    private Consumer<RuntimeDagInstance> scheduleFinished;

    @Override
    public void scheduleFinished(RuntimeDagInstance graph) {
        super.scheduleFinished(graph);
        if (null != scheduleFinished) {
            scheduleFinished.accept(graph);
        }
    }

    public void setScheduleFinished(Consumer<RuntimeDagInstance> scheduleFinished) {
        this.scheduleFinished = scheduleFinished;
    }
}
