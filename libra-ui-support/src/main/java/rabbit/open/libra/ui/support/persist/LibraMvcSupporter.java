package rabbit.open.libra.ui.support.persist;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import rabbit.open.libra.ui.support.persist.entity.TaskExecutionRecord;
import rabbit.open.libra.ui.support.task.WebSupportedSchedulerTask;
import rabbit.open.orm.core.dml.SessionFactory;
import rabbit.open.orm.core.spring.RabbitTransactionManager;

/**
 * web页面支持
 * @author xiaoqianbin
 * @date 2020/7/28
 **/
@Configuration
public class LibraMvcSupporter implements WebMvcConfigurer {

    SessionFactory sessionFactory;

    RabbitTransactionManager transactionManager;

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        // static resources
        registry.addResourceHandler("/libra/*").addResourceLocations("classpath:/META-INF/resources/");
    }

    @Scope("singleton")
    @Bean(initMethod = "setUp", destroyMethod = "destroy")
    public SessionFactory sessionFactory(WebSupportedSchedulerTask webSchedulerTask) {
        if (null == sessionFactory) {
            sessionFactory = new SessionFactory();
            sessionFactory.setDataSource(webSchedulerTask.getDataSource());
            sessionFactory.setShowSql(webSchedulerTask.isShowSql());
            sessionFactory.setDdl(webSchedulerTask.getDdlType().name());
            sessionFactory.setFormatSql(true);
            sessionFactory.setDialect(webSchedulerTask.getDialectType().name());
            sessionFactory.setPackages2Scan(getEntityPackageName());
        }
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
        if (null == transactionManager) {
            transactionManager = new RabbitTransactionManager();
            transactionManager.setSessionFactory(factory);
        }
        return transactionManager;
    }

}
