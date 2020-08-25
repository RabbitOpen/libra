package rabbit.open.libra.test;

import junit.framework.TestCase;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import rabbit.open.libra.client.Constant;
import rabbit.open.libra.client.RegistryConfig;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.dag.DagHeader;
import rabbit.open.libra.client.dag.DagTail;
import rabbit.open.libra.client.dag.DagTaskNode;
import rabbit.open.libra.client.dag.RuntimeDagInstance;
import rabbit.open.libra.client.task.DistributedTask;
import rabbit.open.libra.client.task.SchedulerTask;
import rabbit.open.libra.client.task.TaskSubscriber;
import rabbit.open.libra.dag.schedule.ScheduleContext;
import rabbit.open.libra.test.tasks.MySchedulerTask;

import java.text.SimpleDateFormat;
import java.util.concurrent.Semaphore;

import static rabbit.open.libra.client.Constant.SP;

/**
 * 场景测试
 * @author xiaoqianbin
 * @date 2020/8/25
 **/
@RunWith(JUnit4.class)
public class SceneTest {

    private Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 恢复任务测试
     * @author  xiaoqianbin
     * @date    2020/8/25
     **/
    @Test
    public void recoveringTaskTest() throws InterruptedException {
        TaskSubscriber subscriber = new TaskSubscriber();
        RegistryConfig rc = new RegistryConfig();
        rc.setHosts("localhost:2181");
        rc.setNamespace("/megrez/loan/scene");
        subscriber.setConfig(rc);
        subscriber.init();
        RegistryHelper helper = new RegistryHelper(rc.getHosts(), rc.getNamespace());
        helper.init();
        helper.create(RegistryHelper.META_CONTROLLER + SP + SchedulerTask.class.getSimpleName(),
                "leader", CreateMode.EPHEMERAL);
        // 删除dag节点
        helper.deleteRecursive(RegistryHelper.GRAPHS);
        // 注册任务
        Semaphore s = new Semaphore(0);
        DistributedTask task1 = new DistributedTask() {
            @Override
            public String getTaskName() {
                return "SceneTask-1";
            }

            @Override
            public String getAppName() {
                return "SceneTask";
            }

            @Override
            public void execute(ScheduleContext context) {
                String fireDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(context.getFireDate());
                logger.info("schedule[{}] is started at {}", context.getScheduleId(), fireDate);
                TestCase.assertEquals(0, s.availablePermits());
                s.release();
            }
        };
        task1.setMonitor(subscriber);
        task1.init();
        DistributedTask task2 = new DistributedTask() {
            @Override
            public String getTaskName() {
                return "SceneTask-2";
            }

            @Override
            public String getAppName() {
                return "SceneTask";
            }

            @Override
            public void execute(ScheduleContext context) {
                String fireDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(context.getFireDate());
                logger.info("schedule[{}] is started at {}", context.getScheduleId(), fireDate);
                TestCase.assertEquals(0, s.availablePermits());
                s.release();
            }
        };
        task2.setMonitor(subscriber);
        task2.init();
        DistributedTask task3 = new DistributedTask() {
            @Override
            public String getTaskName() {
                return "SceneTask-3";
            }

            @Override
            public String getAppName() {
                return "SceneTask";
            }

            @Override
            public void execute(ScheduleContext context) {
                String fireDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(context.getFireDate());
                logger.info("schedule[{}] is started at {}", context.getScheduleId(), fireDate);
                TestCase.assertEquals(0, s.availablePermits());
                s.release();
            }
        };
        task3.setMonitor(subscriber);
        task3.init();

        DistributedTask task4 = new DistributedTask() {
            @Override
            public String getTaskName() {
                return "SceneTask-3";
            }

            @Override
            public String getAppName() {
                return "SceneTask";
            }

            @Override
            public void execute(ScheduleContext context) {
                String fireDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(context.getFireDate());
                logger.info("schedule[{}] is started at {}", context.getScheduleId(), fireDate);
                TestCase.assertEquals(0, s.availablePermits());
                s.release();
            }
        };
        task4.setMonitor(subscriber);
        task4.init();
        subscriber.destroy();


        DagHeader header = new DagHeader();
        DagTail tail = new DagTail();
        DagTaskNode n1 = new DagTaskNode(task1);
        DagTaskNode n2 = new DagTaskNode(task2);
        DagTaskNode n3 = new DagTaskNode(task3);
        DagTaskNode n4 = new DagTaskNode(task4);
        header.addNextNode(n1);
        header.addNextNode(n2);
        n1.addNextNode(n3);
        n2.addNextNode(n3);
        n3.addNextNode(n4);
        n4.addNextNode(tail);
        RuntimeDagInstance graph = new RuntimeDagInstance(header, tail, 64);
        String dagId = "recover-graph";
        graph.setDagName("recover-graph");
        graph.setCronExpression("0 * 1 * * *");
        graph.setDagId(dagId);
        // 发布
        helper.create(RegistryHelper.GRAPHS + SP + graph.getDagId(), graph, CreateMode.PERSISTENT);

        // 模拟调度节点上线
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath:/scene");
        context.start();
        subscriber = context.getBean(TaskSubscriber.class);
        task1.setMonitor(subscriber);
        task2.setMonitor(subscriber);
        task3.setMonitor(subscriber);
        task4.setMonitor(subscriber);
        task1.init();
        task2.init();
        task3.init();
        task4.init();
        SchedulerTask schedulerTask = context.getBean(MySchedulerTask.class);

        // 模拟controller掉线
        helper.destroy();
        context.close();

    }
}
