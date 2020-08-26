package rabbit.open.libra.ui.support.annotation;

import org.springframework.context.annotation.Import;
import rabbit.open.libra.ui.support.persist.LibraController;
import rabbit.open.libra.ui.support.persist.LibraMvcSupporter;
import rabbit.open.libra.ui.support.persist.config.LibraConfiguration;
import rabbit.open.libra.ui.support.persist.service.GraphExecutionRecordService;
import rabbit.open.libra.ui.support.persist.service.TaskExecutionRecordService;
import rabbit.open.libra.ui.support.task.WebSupportedSchedulerTask;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * web ui 支持注解
 * @author xiaoqianbin
 * @date 2020/7/28
 **/
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import({LibraMvcSupporter.class, LibraController.class, WebSupportedSchedulerTask.class,
        LibraConfiguration.class,
        GraphExecutionRecordService.class,
        TaskExecutionRecordService.class})
public @interface EnableLibraUserInterface {
}
