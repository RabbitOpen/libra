package rabbit.open.libra.client.task;

import rabbit.open.libra.client.ui.entity.TaskExecutionRecord;
import rabbit.open.libra.client.ui.service.TaskExecutionRecordService;
import rabbit.open.orm.common.dialect.DialectType;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.util.Date;

/**
 * 支持web管理页面的调度管理任务
 * @author xiaoqianbin
 * @date 2020/7/28
 **/
public class WebSchedulerTask extends SchedulerTask {

    private DataSource dataSource;

    private DialectType dialectType;

    @Resource
    private TaskExecutionRecordService recordService;

    @Override
    public void onTaskCompleted(String appName, String group, String taskName, String scheduleTime) {
        TaskExecutionRecord record = recordService.createQuery().addFilter("appName", appName)
                .addFilter("groupName", group)
                .addFilter("scheduleTime", scheduleTime)
                .addFilter("taskName", taskName).unique();
        if (null == record) {
            logger.error("task record[{}-{}-{}-{}] is lost", appName, group, taskName, scheduleTime);
        } else {
            recordService.createUpdate().addFilter("id", record.getId())
                    .set("end", new Date()).execute();
        }
        logger.info("onTaskCompleted [{}-{}-{}-{}]", appName, group, taskName, scheduleTime);
    }

    @Override
    public void onTaskStarted(String appName, String group, String taskName, String scheduleTime) {
        logger.info("onTaskStarted [{}-{}-{}-{}]", appName, group, taskName, scheduleTime);
        TaskExecutionRecord record = new TaskExecutionRecord();
        record.setAppName(appName);
        record.setGroupName(group);
        record.setTaskName(taskName);
        record.setScheduleTime(scheduleTime);
        record.setStart(new Date());
        recordService.add(record);
    }

    public DialectType getDialectType() {
        return dialectType;
    }

    public void setDialectType(String dialectType) {
        this.dialectType = DialectType.format(dialectType);
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

}
