package rabbit.open.libra.test;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import rabbit.open.libra.client.Constant;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.dag.DagTaskNode;
import rabbit.open.libra.client.dag.SchedulableDirectedAcyclicGraph;
import rabbit.open.libra.client.task.SchedulerTask;
import rabbit.open.libra.test.tasks.MySchedulerTask;
import rabbit.open.libra.test.tasks.Task1;

import javax.annotation.Resource;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * @author xiaoqianbin
 * @date 2020/8/14
 **/
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"classpath*:/applicationContext.xml"})
public class DistributedTaskTest implements Serializable {

    @Resource
    MySchedulerTask st;

    @Resource
    SchedulerTask st2;

    @Autowired
    Task1 task1;

    @Test
    public void simpleTest() throws NoSuchFieldException, IllegalAccessException, InterruptedException {
        MyDagTaskNode head = new MyDagTaskNode("head");
        DagTaskNode task = new MyDagTaskNode("task");
        DagTaskNode tail = new MyDagTaskNode("tail");
        head.addNextNode(task);
        task.addNextNode(tail);
        Semaphore s = new Semaphore(0);
        SchedulableDirectedAcyclicGraph dag = new SchedulableDirectedAcyclicGraph(head, tail);
        String dagId = "my-dag-id";
        dag.setDagName("测试dag");
        dag.setCronExpression("0 * * * * *");
        dag.setDagId(dagId);
        st.getRegistryHelper().deleteRecursive(RegistryHelper.GRAPHS + Constant.SP + dag.getDagId());
        st.createDagNode(dag);
        SchedulableDirectedAcyclicGraph o = st.getRegistryHelper().readData(RegistryHelper.GRAPHS + Constant.SP + dag.getDagId());
        TestCase.assertEquals(o.getDagName(), dag.getDagName());
        Map<String, SchedulableDirectedAcyclicGraph>  dagMetaMap = getObjectValue("dagMetaMap", st, SchedulerTask.class);
        st.setLoadMeta(() -> {
            s.release();
        });
        s.acquire();
        TestCase.assertEquals(((MyDagTaskNode)dagMetaMap.get(dagId).getHead()).getName(), "head");
        s.drainPermits();
        head.setName("newHead");
        st.updateDagInfo(dag, () -> {
            s.release();
        });
        s.acquire();
        TestCase.assertEquals(((MyDagTaskNode)dagMetaMap.get(dagId).getHead()).getName(), "newHead");
    }

    /**
     * 反射取值
     * @param	field       字段名
	 * @param	obj         对象
	 * @param	clz         字段所属的类（obj的类或者obj的父类）
     * @author  xiaoqianbin
     * @date    2020/8/20
     **/
    private <T> T getObjectValue(String field, Object obj, Class<?> clz) throws NoSuchFieldException, IllegalAccessException {
        Field f = clz.getDeclaredField(field);
        f.setAccessible(true);
        return (T) f.get(obj);
    }

}
