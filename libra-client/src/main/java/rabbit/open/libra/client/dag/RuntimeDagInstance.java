package rabbit.open.libra.client.dag;

import rabbit.open.libra.client.exception.LibraException;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

/**
 * dag运行态实例
 * @author xiaoqianbin
 * @date 2020/8/20
 **/
@SuppressWarnings("serial")
public class RuntimeDagInstance extends SchedulableDirectedAcyclicGraph {

    private static final long serialVersionUID = 1L;

    /**
     * context
     **/
    private Map<String, Serializable> context = new HashMap<>();

    // 调度id
    private String scheduleId;

    /**
     * 业务调度日期
     */
    private Date scheduleDate;

    private boolean scheduled = false;

    /**
     * 实际调度时间
     */
    private Date fireDate;

    @Override
    public void startSchedule() {
        this.scheduled = true;
        super.startSchedule();
    }

    public boolean isScheduled() {
        return scheduled;
    }

    public RuntimeDagInstance(SchedulableDirectedAcyclicGraph graph) {
        super(graph.getHead(), graph.getTail(), graph.getMaxNodeSize());
        // COPY
        List<Field> fields = getFields(SchedulableDirectedAcyclicGraph.class);
        for (Field field : fields) {
            field.setAccessible(true);
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            try {
                field.set(this, field.get(graph));
            } catch (Exception e) {
                throw new LibraException(e.getMessage());
            }
        }
    }

    private <D> List<Field> getFields(Class<D> clz) {
        List<Field> fields = new ArrayList<>();
        Class<?> sup = clz;
        while (true) {
            fields.addAll(Arrays.asList(clz.getDeclaredFields()));
            if (Object.class.equals(sup.getSuperclass())) {
                break;
            }
            sup = sup.getSuperclass();
        }
        return fields;
    }

    public RuntimeDagInstance(DagTaskNode head, DagTaskNode tail, int maxNodeSize) {
        super(head, tail, maxNodeSize);
    }

    public String getScheduleId() {
        return scheduleId;
    }

    public void setScheduleId(String scheduleId) {
        this.scheduleId = scheduleId;
    }

    public Date getScheduleDate() {
        return scheduleDate;
    }

    public void setScheduleDate(Date scheduleDate) {
        this.scheduleDate = scheduleDate;
    }

    public Date getFireDate() {
        return fireDate;
    }

    public void setFireDate(Date fireDate) {
        this.fireDate = fireDate;
    }

    public void setContext(Map<String, Serializable> context) {
        this.context = context;
    }

    public Map<String, Serializable> getContext() {
        return context;
    }
}
