package rabbit.open.libra.ui.support.persist;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import rabbit.open.libra.ui.support.persist.config.LibraConfiguration;
import rabbit.open.libra.ui.support.persist.entity.TaskExecutionRecord;
import rabbit.open.orm.core.dml.SessionFactory;
import rabbit.open.orm.core.spring.RabbitTransactionManager;
import rabbit.open.orm.datasource.RabbitDataSource;

import javax.sql.DataSource;

/**
 * web页面支持
 * @author xiaoqianbin
 * @date 2020/7/28
 **/
@Configuration
public class LibraMvcSupporter implements WebMvcConfigurer {

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        // static resources
        registry.addResourceHandler("/libra/*").addResourceLocations("classpath:/META-INF/resources/");
    }

    @Scope("singleton")
    @Bean(initMethod = "init", destroyMethod = "shutdown")
    public RabbitDataSource dataSource(LibraConfiguration config) {
        RabbitDataSource dataSource = new RabbitDataSource();
        dataSource.setDriverClass(config.getDriverName());
        dataSource.setUrl(config.getUrl());
        dataSource.setUsername(config.getUsername());
        dataSource.setPassword(config.getPassword());
        dataSource.setMaxIdle(config.getMaxIdleSize());
        dataSource.setMaxSize(config.getMaxSize());
        dataSource.setMinSize(config.getMinSize());
        dataSource.setShowSlowSql(config.isShowSlowSql());
        return dataSource;
    }

    @Scope("singleton")
    @Bean(initMethod = "setUp", destroyMethod = "destroy")
    public SessionFactory sessionFactory(DataSource dataSource, LibraConfiguration config) {
        SessionFactory sessionFactory = new SessionFactory();
        sessionFactory.setDataSource(dataSource);
        sessionFactory.setShowSql(config.isShowSql());
        sessionFactory.setDdl(config.getDdlType());
        sessionFactory.setFormatSql(true);
        sessionFactory.setDialect(config.getDialectType());
        sessionFactory.setPackages2Scan(getEntityPackageName());
        return sessionFactory;
    }

    /**
     * 获取实体类包路径
     * @author xiaoqianbin
     * @date 2020/8/11
     **/
    private String getEntityPackageName() {
        int length = TaskExecutionRecord.class.getSimpleName().length() + 1;
        return TaskExecutionRecord.class.getName().substring(0, TaskExecutionRecord.class.getName().length() - length);
    }

    /**
     * 事务管理器
     * @param    factory
     * @author xiaoqianbin
     * @date 2020/7/30
     **/
    @Scope("singleton")
    @Bean
    public RabbitTransactionManager rabbitTransactionManager(SessionFactory factory) {
        RabbitTransactionManager transactionManager = new RabbitTransactionManager();
        transactionManager.setSessionFactory(factory);
        return transactionManager;
    }

}
