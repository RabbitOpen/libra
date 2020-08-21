package rabbit.open.libra.client.anno;

import org.springframework.context.annotation.Conditional;
import rabbit.open.libra.client.condition.OnBeanCondition;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 排它注解
 * @author xiaoqianbin
 * @date 2020/8/20
 **/
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Conditional(OnBeanCondition.class)
public @interface ConditionalOnMissingBean {
    /**
     * 希望排除的注册
     * @author  xiaoqianbin
     * @date    2020/8/20
     **/
    Class<?> value();
}
