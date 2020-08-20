package rabbit.open.libra.test.tasks;

import org.springframework.stereotype.Component;
import rabbit.open.libra.client.task.SchedulerTask;

/**
 * @author xiaoqianbin
 * @date 2020/8/19
 **/
@Component
public class MySchedulerTask extends SchedulerTask {

    private Runnable loadMeta;

    @Override
    protected void loadDagMeta() {
        super.loadDagMeta();
        if (null != loadMeta) {
            loadMeta.run();
        }
    }

    public void setLoadMeta(Runnable loadMeta) {
        this.loadMeta = loadMeta;
    }

}
