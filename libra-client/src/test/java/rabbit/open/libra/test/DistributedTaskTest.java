package rabbit.open.libra.test;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import rabbit.open.libra.client.Constant;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.dag.DistributedTaskNode;
import rabbit.open.libra.client.dag.SchedulableDag;
import rabbit.open.libra.client.meta.DagMeta;
import rabbit.open.libra.client.task.SchedulerTask;

import javax.annotation.Resource;
import java.io.Serializable;
import java.util.List;

/**
 * @author xiaoqianbin
 * @date 2020/8/14
 **/
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"classpath*:/applicationContext.xml"})
public class DistributedTaskTest implements Serializable {

    @Resource
    SchedulerTask st;

    @Test
    public void simpleTest() {
        DistributedTaskNode head = new MyDistributedTaskNode("head");
        DistributedTaskNode task = new MyDistributedTaskNode("task");
        DistributedTaskNode tail = new MyDistributedTaskNode("tail");
        head.addNextNode(task);
        task.addNextNode(tail);
        SchedulableDag dag = new SchedulableDag(head, tail);
        for (List<DistributedTaskNode> path : dag.getPaths()) {
            System.out.println(path);
        }
        dag.setDagMeta(new DagMeta("测试dag", "my-dag-id", "0 * * * * *"));
        st.getRegistryHelper().deleteRecursive(RegistryHelper.GRAPHS + Constant.SP + dag.getDagMeta().getDagId());
        st.createDagNode(dag);
        SchedulableDag o = st.getRegistryHelper().readData(RegistryHelper.GRAPHS + Constant.SP + dag.getDagMeta().getDagId());
        TestCase.assertEquals(o.getDagMeta().getDagName(), dag.getDagMeta().getDagName());


    }

}
