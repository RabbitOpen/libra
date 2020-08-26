package rabbit.open.libra.ui.support.persist.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import rabbit.open.orm.common.ddl.DDLType;

/**
 * 调度配置类
 * @author xiaoqianbin
 * @date 2020/8/26
 **/
@Component
public class LibraConfiguration {

    /**
     * 数据库方言
     **/
    @Value("${libra.ui.datasource.dialect}")
    private String dialectType;

    /**
     * 是否显示sql
     **/
    @Value("${libra.ui.datasource.showSql:false}")
    private boolean showSql = false;

    /**
     * 是否显示慢sql
     **/
    @Value("${libra.ui.datasource.showSlowSql:false}")
    private boolean showSlowSql = false;

    /**
     * DB建表策略
     **/
    @Value("${libra.ui.datasource.ddlType:UPDATE}")
    private String ddlType = DDLType.UPDATE.name();

    @Value("${libra.ui.datasource.url}")
    private String url;

    @Value("${libra.ui.datasource.username}")
    private String username;

    @Value("${libra.ui.datasource.password}")
    private String password;

    @Value("${libra.ui.datasource.driverName}")
    private String driverName;

    @Value("${libra.ui.datasource.maxSize:10}")
    private int maxSize;

    @Value("${libra.ui.datasource.minSize:3}")
    private int minSize;

    @Value("${libra.ui.datasource.maxIdleSize:3}")
    private int maxIdleSize;

    public String getDialectType() {
        return dialectType;
    }

    public boolean isShowSql() {
        return showSql;
    }

    public boolean isShowSlowSql() {
        return showSlowSql;
    }

    public String getDdlType() {
        return ddlType;
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDriverName() {
        return driverName;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public int getMinSize() {
        return minSize;
    }

    public int getMaxIdleSize() {
        return maxIdleSize;
    }

}
