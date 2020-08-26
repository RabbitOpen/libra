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
import rabbit.open.libra.client.meta.TaskMeta;
import rabbit.open.libra.client.task.DistributedTask;
import rabbit.open.libra.client.task.SchedulerTask;
import rabbit.open.libra.client.task.TaskSubscriber;
import rabbit.open.libra.dag.ScheduleStatus;
import rabbit.open.libra.dag.schedule.ScheduleContext;
import rabbit.open.libra.test.tasks.MySchedulerTask;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;
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
        RegistryConfig rc = new RegistryConfig();
        rc.setHosts("localhost:2181");
        rc.setNamespace("/megrez/loan/scene");
        RegistryHelper helper = new RegistryHelper(rc.getHosts(), rc.getNamespace());
        helper.init();
        helper.deleteRecursive("/meta/tasks");
        helper.deleteRecursive(RegistryHelper.META_CONTROLLER + SP + SchedulerTask.class.getSimpleName());
        helper.create(RegistryHelper.META_CONTROLLER + SP + SchedulerTask.class.getSimpleName(),
                "leader", CreateMode.EPHEMERAL);

        DagHeader header = new DagHeader();
        DagTail tail = new DagTail();

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
                logger.info("task[{}-{}] is started at {}", getTaskName(), context.getTaskId(), fireDate);
                TestCase.assertEquals(0, s.availablePermits());
                s.release(4);

                // 添加一个无关紧要的dag
                RuntimeDagInstance g2 = new RuntimeDagInstance(header, tail, 64);
                g2.setDagName("g2");
                g2.setCronExpression("0 * 2 * * *");
                g2.setDagId("g2");
                monitor.getRegistryHelper().createPersistNode(RegistryHelper.GRAPHS + SP + g2.getDagId(), g2);

            }
        };
        register(helper, task1);

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
                logger.info("task[{}-{}] is started at {}", getTaskName(), context.getTaskId(), fireDate);
                s.release();
            }
        };
        register(helper, task2);

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
                logger.info("task[{}-{}] is started at {}", getTaskName(), context.getTaskId(), fireDate);
                TestCase.assertEquals(4, s.availablePermits());
                s.release(2);
                // 删除g2节点
                monitor.getRegistryHelper().delete(RegistryHelper.GRAPHS + SP + "g2");
            }
        };
        register(helper, task3);

        DistributedTask task4 = new DistributedTask() {
            @Override
            public String getTaskName() {
                return "SceneTask-4";
            }

            @Override
            public String getAppName() {
                return "SceneTask";
            }

            @Override
            public void execute(ScheduleContext context) {
                String fireDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(context.getFireDate());
                logger.info("task[{}-{}] is started at {}", getTaskName(), context.getTaskId(), fireDate);
                TestCase.assertEquals(6, s.availablePermits());
                s.release(3);
            }
        };
        register(helper, task4);

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

        header.setScheduleStatus(ScheduleStatus.FINISHED);
        n1.setScheduleStatus(ScheduleStatus.RUNNING);
        n2.setScheduleStatus(ScheduleStatus.FINISHED);
        graph.getRunningNodes().add(n1);

        // 创建dag
        helper.createPersistNode(RegistryHelper.GRAPHS + SP + graph.getDagId(), graph);
        graph.setScheduleId(UUID.randomUUID().toString().replaceAll("-", ""));
        graph.setFireDate(new Date());
        graph.setScheduleDate(new Date());

        // 发布一个运行中的节点
        helper.createPersistNode(RegistryHelper.GRAPHS + SP + graph.getDagId() + SP + graph.getScheduleId(), graph);

        // 模拟调度节点上线
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath:/scene.xml");
        context.start();
        TaskSubscriber subscriber = context.getBean(TaskSubscriber.class);
        task1.setMonitor(subscriber);
        subscriber.register(task1);
        subscriber.register(task2);
        subscriber.register(task3);
        task3.setMonitor(subscriber);
        subscriber.register(task4);
        MySchedulerTask schedulerTask = context.getBean(MySchedulerTask.class);
        schedulerTask.setScheduleFinished(id -> {
            if (id.equals(dagId)) {
                s.release(10);
                logger.info("dag[{}] finished", id);
            }
        });
        // 模拟controller掉线
        helper.destroy();
        s.acquire(10 + 2 + 3 + 4);
        context.close();
    }

    private void register(RegistryHelper helper, DistributedTask task) {
        String name = task.getAppName() + Constant.SP + task.getTaskName();
        helper.registerTaskMeta(name, new TaskMeta(task), false);
    }

    /**
     * 正常调度测试
     * @author  xiaoqianbin
     * @date    2020/8/25
     **/
    @Test
    public void normalScheduleTest() throws InterruptedException {

        RegistryConfig rc = new RegistryConfig();
        rc.setHosts("localhost:2181");
        rc.setNamespace("/megrez/loan/scene");
        RegistryHelper helper = new RegistryHelper(rc.getHosts(), rc.getNamespace());
        helper.init();
        helper.deleteRecursive("/meta/tasks");
        helper.deleteRecursive(RegistryHelper.META_CONTROLLER + SP + SchedulerTask.class.getSimpleName());
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
                logger.info("task[{}-{}] is started at {}", getTaskName(), context.getTaskId(), fireDate);
                s.release(4);
            }
        };
        register(helper, task1);

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
                logger.info("task[{}-{}] is started at {}", getTaskName(), context.getTaskId(), fireDate);
                s.release();
            }
        };
        register(helper, task2);

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
                logger.info("task[{}-{}] is started at {}", getTaskName(), context.getTaskId(), fireDate);
                TestCase.assertEquals(5, s.availablePermits());
                s.release(2);
            }
        };
        register(helper, task3);

        DistributedTask task4 = new DistributedTask() {
            @Override
            public String getTaskName() {
                return "SceneTask-4";
            }

            @Override
            public String getAppName() {
                return "SceneTask";
            }

            @Override
            public void execute(ScheduleContext context) {
                String fireDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(context.getFireDate());
                logger.info("task[{}-{}] is started at {}", getTaskName(), context.getTaskId(), fireDate);
                TestCase.assertEquals(7, s.availablePermits());
                s.release(3);
            }
        };
        register(helper, task4);

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
        Date fireDate = new Date();
        fireDate.setTime(fireDate.getTime() - 1000);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        RuntimeDagInstance graph = new RuntimeDagInstance(header, tail, 64);
        Calendar c = Calendar.getInstance();
        String dagId = "recover-graph";
        graph.setDagName("recover-graph");
        Date now = c.getTime();
        now.setTime(now.getTime() - 2000);
        c.setTime(now);
        String cronExpression = String.format("%d %d %d * * *", c.get(Calendar.SECOND), c.get(Calendar.MINUTE), c.get(Calendar.HOUR_OF_DAY));
        graph.setCronExpression(cronExpression);
        logger.info("cronExpression: {}", cronExpression);
        now.setTime(now.getTime() - 2000 * 20);
        graph.setDagId(dagId);
        // 设置上次触发时间
        graph.setLastFireDate(now);
        // 创建dag
        helper.createPersistNode(RegistryHelper.GRAPHS + SP + graph.getDagId(), graph);
        graph.setScheduleId(UUID.randomUUID().toString().replaceAll("-", ""));
        graph.setFireDate(new Date());
        graph.setScheduleDate(new Date());


        // 模拟调度节点上线
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath:/scene.xml");
        context.start();
        TaskSubscriber subscriber = context.getBean(TaskSubscriber.class);
        subscriber.register(task1);
        subscriber.register(task2);
        subscriber.register(task3);
        subscriber.register(task4);
        MySchedulerTask schedulerTask = context.getBean(MySchedulerTask.class);
        schedulerTask.setScheduleFinished(id -> {
            if (id.equals(dagId)) {
                s.release(10);
                logger.info("dag[{}] finished", id);
            }
        });
        // 模拟controller掉线
        helper.destroy();
        s.acquire(10 + 1 + 2 + 3 + 4);
        TestCase.assertEquals(0, s.availablePermits());
        context.close();

    }
}
