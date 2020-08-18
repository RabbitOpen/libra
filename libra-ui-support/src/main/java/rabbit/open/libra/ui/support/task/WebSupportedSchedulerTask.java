package rabbit.open.libra.ui.support.task;

import rabbit.open.libra.ui.support.persist.service.TaskExecutionRecordService;
import rabbit.open.orm.common.ddl.DDLType;
import rabbit.open.orm.common.dialect.DialectType;

import javax.annotation.Resource;
import javax.sql.DataSource;

/**
 * 支持web管理页面的调度管理任务
 * @author xiaoqianbin
 * @date 2020/7/28
 **/
public class WebSupportedSchedulerTask {

    private DataSource dataSource;

    private DialectType dialectType;

    private boolean showSql = false;

    private DDLType ddlType = DDLType.UPDATE;

    @Resource
    private TaskExecutionRecordService recordService;

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

    public boolean isShowSql() {
        return showSql;
    }

    public void setShowSql(boolean showSql) {
        this.showSql = showSql;
    }

    public DDLType getDdlType() {
        return ddlType;
    }

    public void setDdlType(String ddlType) {
        this.ddlType = DDLType.valueOf(ddlType.toUpperCase());
    }
}
