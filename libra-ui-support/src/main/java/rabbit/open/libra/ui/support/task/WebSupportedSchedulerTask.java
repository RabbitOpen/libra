package rabbit.open.libra.ui.support.task;

import rabbit.open.libra.client.anno.ConditionalOnMissingBeanType;
import rabbit.open.libra.client.dag.RuntimeDagInstance;
import rabbit.open.libra.client.meta.TaskExecutionContext;
import rabbit.open.libra.client.task.SchedulerTask;
import rabbit.open.libra.dag.ScheduleStatus;
import rabbit.open.libra.ui.support.persist.entity.GraphExecutionRecord;
import rabbit.open.libra.ui.support.persist.entity.TaskExecutionRecord;
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

    /**
     * 开始调度
     * @param	graph
     * @author  xiaoqianbin
     * @date    2020/8/26
     **/
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

    /**
     * 调度完成
     * @param	graph
     * @author  xiaoqianbin
     * @date    2020/8/26
     **/
    @Override
    public void scheduleFinished(RuntimeDagInstance graph) {
        GraphExecutionRecord record = graphService.getByID(graph.getScheduleId());
        if (null != record) {
            record.setEnd(new Date());
            record.setStatus(ScheduleStatus.FINISHED);
            graphService.updateByID(record);
        } else {
            logger.error("graph record[{}] is not existed!", graph.getScheduleId());
        }
        super.scheduleFinished(graph);
    }

    /**
     * 任务完成了
     * @param	ctx
     * @author  xiaoqianbin
     * @date    2020/8/26
     **/
    @Override
    public void onTaskCompleted(TaskExecutionContext ctx) {
        TaskExecutionRecord record = recordService.getByID(ctx.getTaskId());
        if (null == record) {
            logger.error("task record[{}] is not found!", ctx.getTaskId());
        } else {
            record.setEnd(new Date());
            record.setStatus(ScheduleStatus.FINISHED);
            record.setSuccess(ctx.getSplitsCount());
            recordService.updateByID(record);
        }
        super.onTaskCompleted(ctx);
    }

    /**
     * 任务发布了
     * @param	ctx
     * @author  xiaoqianbin
     * @date    2020/8/26
     **/
    @Override
    public void onTaskPublished(TaskExecutionContext ctx) {
        TaskExecutionRecord record = new TaskExecutionRecord();
        record.setAppName(ctx.getAppName());
        record.setScheduleId(ctx.getScheduleId());
        record.setTaskName(ctx.getTaskName());
        record.setTaskId(ctx.getTaskId());
        record.setId(ctx.getTaskId());
        record.setStart(new Date());
        record.setScheduleDate(ctx.getScheduleDate());
        record.setFireDate(ctx.getFireDate());
        record.setSplitsCount(ctx.getSplitsCount());
        record.setSuccess(0);
        record.setFailed(0);
        recordService.add(record);
        super.onTaskPublished(ctx);
    }
}
