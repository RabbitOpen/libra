package rabbit.open.libra.client.task;

import rabbit.open.orm.common.dialect.DialectType;

import javax.sql.DataSource;

/**
 * 支持web管理页面的调度管理任务
 * @author xiaoqianbin
 * @date 2020/7/28
 **/
public class WebSchedulerTask extends SchedulerTask {

    private DataSource dataSource;

    private DialectType dialectType;

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
