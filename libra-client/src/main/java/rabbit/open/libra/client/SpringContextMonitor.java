package rabbit.open.libra.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationContextEvent;
import org.springframework.context.event.ContextRefreshedEvent;

/**
 * spring context 监视器
 * @author xiaoqianbin
 * @date 2020/7/10
 **/
public class SpringContextMonitor implements ApplicationListener<ApplicationContextEvent> {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void onApplicationEvent(ApplicationContextEvent event) {
        if (event instanceof ContextRefreshedEvent) {
            logger.info("begin run distributed tasks");
            AbstractLibraTask.runScheduleTasks();
        } else {
            logger.info("distributed task scheduling is exited");
            AbstractLibraTask.shutdown();
        }
    }

}
