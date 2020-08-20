package rabbit.open.libra.test;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import rabbit.open.libra.client.Constant;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.dag.DistributedTaskNode;
import rabbit.open.libra.client.dag.SchedulableDag;
import rabbit.open.libra.client.meta.DagMeta;
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
        MyDistributedTaskNode head = new MyDistributedTaskNode("head");
        DistributedTaskNode task = new MyDistributedTaskNode("task");
        DistributedTaskNode tail = new MyDistributedTaskNode("tail");
        head.addNextNode(task);
        task.addNextNode(tail);
        Semaphore s = new Semaphore(0);
        SchedulableDag dag = new SchedulableDag(head, tail);
        String dagId = "my-dag-id";
        dag.setDagMeta(new DagMeta("测试dag", dagId, "0 * * * * *"));
        st.getRegistryHelper().deleteRecursive(RegistryHelper.GRAPHS + Constant.SP + dag.getDagMeta().getDagId());
        st.createDagNode(dag);
        SchedulableDag o = st.getRegistryHelper().readData(RegistryHelper.GRAPHS + Constant.SP + dag.getDagMeta().getDagId());
        TestCase.assertEquals(o.getDagMeta().getDagName(), dag.getDagMeta().getDagName());
        Map<String, SchedulableDag>  dagMetaMap = getObjectValue("dagMetaMap", st, SchedulerTask.class);
        st.setLoadMeta(() -> {
            s.release();
        });
        s.acquire();
        TestCase.assertEquals(((MyDistributedTaskNode)dagMetaMap.get(dagId).getHead()).getName(), "head");
    }

    public <T> T getObjectValue(String field, Object obj, Class<?> clz) throws NoSuchFieldException, IllegalAccessException {
        Field f = clz.getDeclaredField(field);
        f.setAccessible(true);
        return (T) f.get(obj);
    }

}
