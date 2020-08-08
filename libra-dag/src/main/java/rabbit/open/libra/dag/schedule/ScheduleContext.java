package rabbit.open.libra.dag.schedule;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 调度上下文
 * @author xiaoqianbin
 * @date 2020/8/8
 **/
public class ScheduleContext {

    Map<String, Serializable> context = new HashMap<>();

    public void setContext(String key, Serializable value) {
        this.context.put(key, value);
    }
}
