package rabbit.open.libra.client.annotation;

import org.springframework.context.annotation.Import;
import rabbit.open.libra.client.ui.LibraController;
import rabbit.open.libra.client.ui.LibraMvcSupporter;
import rabbit.open.libra.client.ui.service.TaskExecutionRecordService;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * libra web UI enable annotation
 * @author xiaoqianbin
 * @date 2020/7/28
 **/
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import({LibraMvcSupporter.class, LibraController.class,
        TaskExecutionRecordService.class})
public @interface EnableLibraWebUI {
}
