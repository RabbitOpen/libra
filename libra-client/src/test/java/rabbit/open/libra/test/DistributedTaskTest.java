package rabbit.open.libra.test;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import rabbit.open.libra.client.Constant;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.dag.*;
import rabbit.open.libra.client.exception.RepeatedScheduleException;
import rabbit.open.libra.client.task.DistributedTask;
import rabbit.open.libra.client.task.SchedulerTask;
import rabbit.open.libra.client.task.TaskSubscriber;
import rabbit.open.libra.dag.schedule.ScheduleContext;
import rabbit.open.libra.test.tasks.SampleDagTaskNode;
import rabbit.open.libra.test.tasks.MySchedulerTask;
import rabbit.open.libra.test.tasks.SimpleTask;

import javax.annotation.Resource;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * 分布式任务测试
 * @author xiaoqianbin
 * @date 2020/8/14
 **/
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"classpath*:/distributed-task.xml"})
public class DistributedTaskTest {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Resource
    MySchedulerTask mySchedulerTask;

    @Resource
    SchedulerTask schedulerTask;

    @Autowired
    SimpleTask task;

    /**
     * <b>@description 简单测试 </b>
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     * @throws InterruptedException
     */
    @Test
    public void simpleTest() throws NoSuchFieldException, IllegalAccessException, InterruptedException {
        TestCase.assertEquals(schedulerTask, mySchedulerTask);
        SampleDagTaskNode head = new SampleDagTaskNode("head");
        DagTaskNode task = new SampleDagTaskNode("task");
        DagTaskNode tail = new SampleDagTaskNode("tail");
        head.addNextNode(task);
        task.addNextNode(tail);
        Semaphore s = new Semaphore(0);
        SchedulableDirectedAcyclicGraph dag = new SchedulableDirectedAcyclicGraph(head, tail);
        String dagId = "my-dag-id";
        dag.setDagName("测试dag");
        dag.setCronExpression("0 * 1 * * *");
        dag.setDagId(dagId);
        mySchedulerTask.getRegistryHelper().deleteRecursive(RegistryHelper.GRAPHS + Constant.SP + dag.getDagId());
        mySchedulerTask.createGraphNode(dag);
        SchedulableDirectedAcyclicGraph o = mySchedulerTask.getRegistryHelper()
                .readData(RegistryHelper.GRAPHS + Constant.SP + dag.getDagId());
        TestCase.assertEquals(o.getDagName(), dag.getDagName());
        Map<String, SchedulableDirectedAcyclicGraph> dagMetaMap = getObjectValue("dagMetaMap", mySchedulerTask,
                SchedulerTask.class);
        mySchedulerTask.setLoadMeta(() -> {
            s.release();
        });
        s.acquire();
        TestCase.assertEquals("head", dagMetaMap.get(dagId).getHead().getTaskName());
        s.drainPermits();
        head.setTaskName("newHead");
        mySchedulerTask.updateDagInfo(dag, () -> {
            s.release();
        });
        s.acquire();
        TestCase.assertEquals(dagMetaMap.get(dagId).getHead().getTaskName(), "newHead");
    }

    /**
     * 反射取值
     * @param field 字段名
     * @param obj   对象
     * @param clz   字段所属的类（obj的类或者obj的父类）
     * @author xiaoqianbin
     * @date 2020/8/20
     **/
    @SuppressWarnings("unchecked")
    private <T> T getObjectValue(String field, Object obj, Class<?> clz)
            throws NoSuchFieldException, IllegalAccessException {
        Field f = clz.getDeclaredField(field);
        f.setAccessible(true);
        return (T) f.get(obj);
    }

    /**
     * 简单调度
     * @author xiaoqianbin
     * @date 2020/8/25
     **/
    @Test
    public void scheduleTest() throws InterruptedException, NoSuchFieldException, IllegalAccessException {
        DagHeader header = new DagHeader();
        DagTail tail = new DagTail();
        Semaphore quit = new Semaphore(0);
        task.setTask(ctx -> {
            logger.info("schedule date: {}", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ctx.getScheduleDate()));
            quit.release();
        });
        mySchedulerTask.setScheduleFinished(dagId -> {
            if (dagId.equals("schedule-test-graph")) {
                quit.release();
            }
        });
        DagTaskNode taskNode = new DagTaskNode(task.getTaskName(), 2, task.getSplitsCount(), task.getAppName());
        header.addNextNode(taskNode);
        taskNode.addNextNode(tail);
        RuntimeDagInstance graph = new RuntimeDagInstance(header, tail, 64);
        String dagId = "schedule-test-graph";
        graph.setDagName("测试dag");
        graph.setCronExpression("0 * 1 * * *");
        graph.setDagId(dagId);
        mySchedulerTask.getRegistryHelper().deleteRecursive(RegistryHelper.GRAPHS + Constant.SP + graph.getDagId());
        logger.info("**** delete dag {}", dagId);
        Map<String, SchedulableDirectedAcyclicGraph> dagMetaMap = getObjectValue("dagMetaMap", mySchedulerTask,
                SchedulerTask.class);
        dagMetaMap.remove(graph.getDagId());
        mySchedulerTask.createGraphNode(graph);
        logger.info("**** create dag {}", dagId);
        mySchedulerTask.scheduleGraph(graph);
        logger.info("**** schedule dag {}", dagId);
        try {
            mySchedulerTask.scheduleGraph(graph);
            throw new RuntimeException("不可能走到这里");
        } catch (RepeatedScheduleException e) {

        }
        quit.acquire(2);
    }


    @Autowired
    TaskSubscriber subscriber;

    /**
     * 复杂调度测试
     * @author xiaoqianbin
     * @date 2020/8/25
     **/
    @Test
    public void complexScheduleTest() throws InterruptedException {
        HashMap<String, Serializable> context = new HashMap<>();
        final String value = "context-value";
        context.put("context", value);
        Semaphore s = new Semaphore(0);
        mySchedulerTask.setScheduleFinished(dagId -> {
            if (dagId.equals("complex-graph")) {
                s.release();
                logger.info("schedule[complex-graph] is finished");
            }
        });
        DistributedTask task1 = new DistributedTask() {
            @Override
            public String getTaskName() {
                return "DistributedTask-1";
            }

            @Override
            public String getAppName() {
                return "complexSchedule-1";
            }

            @Override
            public void execute(ScheduleContext context) {
                String fireDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(context.getFireDate());
                logger.info("schedule[{}] is started at {}", context.getScheduleId(), fireDate);
                TestCase.assertEquals(0, s.availablePermits());
                s.release();
                TestCase.assertEquals(context.getContextValue("context"), value);
            }
        };
        task1.setMonitor(subscriber);
        task1.init();

        DistributedTask task2 = new DistributedTask() {
            @Override
            public String getTaskName() {
                return "DistributedTask-2";
            }

            @Override
            public String getAppName() {
                return "complexSchedule-1";
            }

            @Override
            public void execute(ScheduleContext context) {
                String fireDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(context.getFireDate());
                logger.info("task[{}][{}-{}] is started at {}", getTaskName(), context.getScheduleId(), context.getIndex(), fireDate);
                s.release();
                TestCase.assertEquals(context.getContextValue("context"), value);
            }

            @Override
            public int getSplitsCount() {
                return 2;
            }
        };
        task2.setMonitor(subscriber);
        task2.init();

        DistributedTask task3 = new DistributedTask() {
            @Override
            public String getTaskName() {
                return "DistributedTask-3";
            }

            @Override
            public String getAppName() {
                return "complexSchedule-2";
            }

            @Override
            public int getSplitsCount() {
                return 3;
            }

            @Override
            public int getParallel() {
                return 2;
            }

            @Override
            public void execute(ScheduleContext context) {
                String fireDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(context.getFireDate());
                logger.info("task[{}][{}-{}] is started at {}", getTaskName(), context.getScheduleId(), context.getIndex(), fireDate);
                s.release();
                TestCase.assertEquals(context.getContextValue("context"), value);
            }
        };
        task3.setMonitor(subscriber);
        task3.init();

        DistributedTask task4 = new DistributedTask() {
            @Override
            public String getTaskName() {
                return "DistributedTask-4";
            }

            @Override
            public String getAppName() {
                return "complexSchedule-2";
            }

            @Override
            public void execute(ScheduleContext context) {
                String fireDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(context.getFireDate());
                logger.info("task[{}][{}-{}] is started at {}", getTaskName(), context.getScheduleId(), context.getIndex(), fireDate);
                TestCase.assertEquals(1 + 2 + 3, s.availablePermits());
                s.release();
                TestCase.assertEquals(context.getContextValue("context"), value);
            }
        };
        task4.setMonitor(subscriber);
        task4.init();

        DagHeader header = new DagHeader();
        DagTail tail = new DagTail();
        DagTaskNode n1 = new DagTaskNode(task1);
        DagTaskNode n2 = new DagTaskNode(task2);
        DagTaskNode n3 = new DagTaskNode(task3);
        DagTaskNode n4 = new DagTaskNode(task4);
        header.addNextNode(n1);
        n1.addNextNode(n2);
        n1.addNextNode(n3);
        n2.addNextNode(n4);
        n3.addNextNode(n4);
        n4.addNextNode(tail);

        RuntimeDagInstance graph = new RuntimeDagInstance(header, tail, 64);
        String dagId = "complex-graph";
        graph.setDagName("complex-graph");
        graph.setCronExpression("0 * 1 * * *");
        graph.setDagId(dagId);
        graph.setContext(context);
        mySchedulerTask.getRegistryHelper().deleteRecursive(RegistryHelper.GRAPHS + Constant.SP + graph.getDagId());
        mySchedulerTask.createGraphNode(graph);
        mySchedulerTask.scheduleGraph(graph);
        s.acquire(1 + task1.getSplitsCount() + task2.getSplitsCount() + task3.getSplitsCount() + task4.getSplitsCount());
    }
}
