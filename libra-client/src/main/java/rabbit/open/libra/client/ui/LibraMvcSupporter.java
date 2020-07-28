package rabbit.open.libra.client.ui;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import rabbit.open.libra.client.task.WebSchedulerTask;
import rabbit.open.orm.common.ddl.DDLType;
import rabbit.open.orm.core.dml.SessionFactory;
import rabbit.open.orm.core.spring.RabbitTransactionManager;

import javax.annotation.Resource;

/**
 * web页面支持
 * @author xiaoqianbin
 * @date 2020/7/28
 **/
@Configuration
public class LibraMvcSupporter implements WebMvcConfigurer {

    @Resource
    private WebSchedulerTask webSchedulerTask;

    SessionFactory sessionFactory;

    RabbitTransactionManager transactionManager;

    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        // static resources
        registry.addResourceHandler("/libra/*").addResourceLocations("classpath:/META-INF/resources/");
    }

    @Bean(initMethod = "setUp", destroyMethod = "destroy")
    public SessionFactory sessionFactory() {
        if (null == sessionFactory) {
            sessionFactory = new SessionFactory();
            sessionFactory.setDataSource(webSchedulerTask.getDataSource());
            sessionFactory.setShowSql(true);
            sessionFactory.setDdl(DDLType.UPDATE.name());
            sessionFactory.setFormatSql(true);
            sessionFactory.setDialect(webSchedulerTask.getDialectType().name());
            sessionFactory.setPackages2Scan("rabbit.open.libra.client.ui");
        }
        return sessionFactory;
    }

    @Bean
    public RabbitTransactionManager transactionManager(SessionFactory factory) {
        if (null == transactionManager) {
            transactionManager = new RabbitTransactionManager();
            transactionManager.setSessionFactory(factory);
        }
        return transactionManager;
    }

}
