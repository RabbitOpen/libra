package rabbit.open.test;

import junit.framework.TestCase;
import org.I0Itec.zkclient.IZkChildListener;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.TaskMeta;
import rabbit.open.libra.client.exception.LibraException;
import rabbit.open.libra.client.meta.ScheduleContext;
import rabbit.open.libra.client.task.DistributedTask;
import rabbit.open.libra.client.task.SchedulerTask;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * @author xiaoqianbin
 * @date 2020/7/13
 **/
@RunWith(JUnit4.class)
public class LibraTest {

    private Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 读写测试
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    @Test
    public void helperTest() {
        RegistryHelper helper = new RegistryHelper();
        helper.setHosts("localhost:2181");
        helper.setNamespace("/libra/root");
        helper.init();
        String path = "/hello/li/si/sd";
        TestCase.assertTrue(!helper.exists(path));
        helper.createPersistNode(path);
        TestCase.assertTrue(helper.exists(path));
        helper.deleteNode("/hello");
        TestCase.assertTrue(!helper.exists(path));
        helper.destroy();
    }

    int count = 0;

    /**
     * 事件注册
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    @Test
    public void eventTest() throws InterruptedException {
        RegistryHelper helper = new RegistryHelper();
        helper.setHosts("localhost:2181");
        helper.setNamespace("/libra/root");
        helper.init();
        helper.deleteNode("/event");
        helper.createPersistNode("/event");
        Semaphore semaphore = new Semaphore(0);
        IZkChildListener listener = (s, list) -> {
            count++;
            logger.info("child changed {}", count);
            semaphore.release();
        };
        helper.subscribeChildChanges("/event", listener);
        helper.createPersistNode("/event/1");
        helper.createPersistNode("/event/2");
        semaphore.acquire(2);
        TestCase.assertEquals(2, count);
        helper.unsubscribeChildChanges("/event", listener);
        helper.createPersistNode("/event/3");
        semaphore.tryAcquire(3, TimeUnit.SECONDS);
        TestCase.assertEquals(2, count);
        helper.deleteNode("/event");
        helper.destroy();
    }

    /**
     * 并发测试
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    @Test
    public void multiThreadTest() throws InterruptedException {
        RegistryHelper helper = new RegistryHelper();
        helper.setHosts("localhost:2181");
        helper.setNamespace("/libra/root");
        helper.init();
        int count = 20;
        CountDownLatch cdl = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            new Thread(() -> {
                try {
                    helper.createPersistNode("/hellowork/" + Thread.currentThread().getName(), true);
                } catch (LibraException e) {

                }
                logger.info("node 【/hellowork/{}】 created！ ", Thread.currentThread().getName());
                cdl.countDown();
            }).start();
        }
        cdl.await();
        TestCase.assertTrue(helper.getChildren("/hellowork").size() == count);
        helper.deleteNode("/hellowork");
        TestCase.assertTrue(!helper.exists("/hellowork"));
        helper.destroy();
    }

    @Test
    public void selectLastTaskTest() {
        SchedulerTaskTest st = new SchedulerTaskTest();

        List<String> task = new ArrayList<>();
        task.add("MyTask");
        List<TaskMeta> metaList = new ArrayList<>();
        metaList.add(new TaskMeta(new MyTask()));
        String latestTask = st.getLatestTask(task, metaList);
        TestCase.assertEquals("MyTask", latestTask);

        task.add("MyTask3");
        task.add("MyTask4");
        metaList.add(new TaskMeta(new MyTask()));
        metaList.add(new TaskMeta(new MyTask2()));
        metaList.add(new TaskMeta(new MyTask4()));
        metaList.add(new TaskMeta(new MyTask3()));

        latestTask = st.getLatestTask(task, metaList);
        TestCase.assertEquals("MyTask3", latestTask);

    }


    public class SchedulerTaskTest extends SchedulerTask {
        @Override
        public String getLatestTask(List<String> tasks, List<TaskMeta> metaList) {
            return super.getLatestTask(tasks, metaList);
        }
    }

    public static class MyTask extends DistributedTask {
        @Override
        public RegistryHelper getRegistryHelper() {
            return null;
        }

        @Override
        public void execute(ScheduleContext context) {

        }

        @Override
        protected String getCronExpression() {
            return null;
        }
    }

    public static class MyTask4 extends MyTask {
    }

    public static class MyTask2 extends MyTask {
    }

    public static class MyTask3 extends MyTask {
    }
}
