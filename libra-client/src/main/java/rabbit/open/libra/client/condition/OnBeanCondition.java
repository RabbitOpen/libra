package rabbit.open.libra.client.condition;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import rabbit.open.libra.client.anno.ConditionalOnMissingBeanType;

import java.util.Map;

/**
 * 排它条件
 * @author xiaoqianbin
 * @date 2020/8/20
 **/
public class OnBeanCondition implements Condition {

    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        try {
            BeanDefinitionRegistry registry = context.getRegistry();
            Map<String, Object> attributes = metadata.getAnnotationAttributes(ConditionalOnMissingBeanType.class.getName());
            Class<?> clz = (Class<?>) attributes.get("type");
            for (String definitionName : registry.getBeanDefinitionNames()) {
                BeanDefinition definition = registry.getBeanDefinition(definitionName);
                String clzName = definition.getBeanClassName();
                if (clz.isAssignableFrom(Class.forName(clzName))) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
