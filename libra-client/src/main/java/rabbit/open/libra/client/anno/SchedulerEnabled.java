package rabbit.open.libra.client.anno;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import rabbit.open.libra.client.task.SchedulerTask;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 开启调度节点
 * @author xiaoqianbin
 * @date 2020/8/16
 **/
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Configuration
@Import({SchedulerTask.class})
public @interface SchedulerEnabled {
}
