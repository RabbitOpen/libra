package rabbit.open.libra.client.anno;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import rabbit.open.libra.client.TaskSubscriber;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 客户端分布式任务启动注解
 * @author xiaoqianbin
 * @date 2020/8/14
 **/
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Configuration
@Import({TaskSubscriber.class})
public @interface DistributedTaskEnabled {

}
