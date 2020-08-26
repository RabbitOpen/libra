package rabbit.open.libra.ui.support.task;

import rabbit.open.libra.client.anno.ConditionalOnMissingBeanType;
import rabbit.open.libra.client.dag.RuntimeDagInstance;
import rabbit.open.libra.client.task.SchedulerTask;
import rabbit.open.libra.ui.support.persist.entity.GraphExecutionRecord;
import rabbit.open.libra.ui.support.persist.service.GraphExecutionRecordService;
import rabbit.open.libra.ui.support.persist.service.TaskExecutionRecordService;

import javax.annotation.Resource;
import java.util.Date;

/**
 * 支持web管理页面的调度管理任务
 * @author xiaoqianbin
 * @date 2020/7/28
 **/
@ConditionalOnMissingBeanType(type = WebSupportedSchedulerTask.class)
public class WebSupportedSchedulerTask extends SchedulerTask {

    @Resource
    private TaskExecutionRecordService recordService;

    @Resource
    private GraphExecutionRecordService graphService;

    @Override
    protected synchronized boolean startSchedule(RuntimeDagInstance graph) {
        boolean result = super.startSchedule(graph);
        if (result) {
            GraphExecutionRecord record = new GraphExecutionRecord();
            record.setDagId(graph.getDagId());
            record.setScheduleDate(graph.getScheduleDate());
            record.setFireDate(graph.getFireDate());
            record.setTaskNodeNum(graph.getNodes().size() - 2);
            record.setStart(graph.getFireDate());
            record.setId(graph.getScheduleId());
            record.setScheduleId(graph.getScheduleId());
            graphService.add(record);
        }
        return result;
    }

    @Override
    public void scheduleFinished(RuntimeDagInstance graph) {
        GraphExecutionRecord record = graphService.getByID(graph.getScheduleId());
        if (null != record) {
            record.setEnd(new Date());
            graphService.updateByID(record);
        } else {
            logger.error("graph record[{}] is not existed!", graph.getScheduleId());
        }
        super.scheduleFinished(graph);
    }
}
