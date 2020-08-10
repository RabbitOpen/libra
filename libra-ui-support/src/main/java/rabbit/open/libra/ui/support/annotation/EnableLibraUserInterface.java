package rabbit.open.libra.ui.support.annotation;

import org.springframework.context.annotation.Import;
import rabbit.open.libra.client.SpringContextMonitor;
import rabbit.open.libra.ui.support.persist.LibraController;
import rabbit.open.libra.ui.support.persist.LibraMvcSupporter;
import rabbit.open.libra.ui.support.persist.service.TaskExecutionRecordService;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * libra web UI support annotation
 * @author xiaoqianbin
 * @date 2020/7/28
 **/
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import({LibraMvcSupporter.class, LibraController.class, SpringContextMonitor.class,
        TaskExecutionRecordService.class})
public @interface EnableLibraUserInterface {
}
