package rabbit.open.libra.test.tasks;

import org.springframework.stereotype.Component;
import rabbit.open.libra.client.dag.SchedulableDirectedAcyclicGraph;
import rabbit.open.libra.client.task.SchedulerTask;

/**
 * @author xiaoqianbin
 * @date 2020/8/19
 **/
@Component
public class MySchedulerTask extends SchedulerTask {

    private Runnable loadMeta;

    private Runnable updateCallback;

    @Override
    protected void loadDagMetas() {
        super.loadDagMetas();
        if (null != loadMeta) {
            loadMeta.run();
        }
    }

    public void setLoadMeta(Runnable loadMeta) {
        this.loadMeta = loadMeta;
    }

    public void updateDagInfo(SchedulableDirectedAcyclicGraph dag, Runnable r) {
        super.updateDagInfo(dag);
        this.updateCallback = r;
    }

    @Override
    protected void updateDagMetaMap(String key, SchedulableDirectedAcyclicGraph dag) {
        super.updateDagMetaMap(key, dag);
        if (null != updateCallback) {
            updateCallback.run();
        }
    }
}
