package rabbit.open.libra.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ApplicationContextEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

/**
 * spring context 监视器
 * @author xiaoqianbin
 * @date 2020/7/10
 **/
@Component
public class SpringContextMonitor implements ApplicationListener<ApplicationContextEvent> {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void onApplicationEvent(ApplicationContextEvent event) {
        if (event instanceof ContextRefreshedEvent) {
            logger.info("开启动分布式任务调度");
            AbstractLibraTask.runScheduleTasks();
        } else {
            logger.info("退出分布式任务调度");
            AbstractLibraTask.shutdown();
        }
    }

}
