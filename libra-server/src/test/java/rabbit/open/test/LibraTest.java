package rabbit.open.test;

import junit.framework.TestCase;
import org.I0Itec.zkclient.IZkChildListener;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rabbit.open.libra.client.RegistryHelper;

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
     * @author  xiaoqianbin
     * @date    2020/7/14
     **/
    @Test
    public void helperTest() {
        RegistryHelper helper = new RegistryHelper();
        helper.init();
        String path = "/hello/li/si/sd";
        TestCase.assertTrue(!helper.getClient().exists(path));
        helper.createPersistNode(path);
        TestCase.assertTrue(helper.getClient().exists(path));
        helper.deleteNode("/hello");
        TestCase.assertTrue(!helper.getClient().exists(path));
        helper.destroy();
    }

    int count = 0;

    /**
     * 事件注册
     * @author  xiaoqianbin
     * @date    2020/7/14
     **/
    @Test
    public void eventTest() throws InterruptedException {
        RegistryHelper helper = new RegistryHelper();
        helper.init();
        helper.deleteNode("/event");
        helper.createPersistNode("/event");
        Semaphore semaphore = new Semaphore(0);
        IZkChildListener listener = (s, list) -> {
            count++;
            logger.info("child changed {}", count);
            semaphore.release();
        };
        helper.getClient().subscribeChildChanges("/event", listener);
        helper.createPersistNode("/event/1");
        helper.createPersistNode("/event/2");
        semaphore.acquire(2);
        TestCase.assertEquals(2, count);
        helper.getClient().unsubscribeChildChanges("/event", listener);
        helper.createPersistNode("/event/3");
        semaphore.tryAcquire(3, TimeUnit.SECONDS);
        TestCase.assertEquals(2, count);
        helper.deleteNode("/event");
        helper.destroy();
    }

    /**
     * 并发测试
     * @author  xiaoqianbin
     * @date    2020/7/14
     **/
    @Test
    public void multiThreadTest() throws InterruptedException {
        RegistryHelper helper = new RegistryHelper();
        helper.init();
        int count = 20;
        CountDownLatch cdl = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            new Thread(() -> {
                helper.createPersistNode("/hellowork/" + Thread.currentThread().getName());
                logger.info("node 【/hellowork/{}】 created！ ", Thread.currentThread().getName());
                cdl.countDown();
            }).start();
        }
        cdl.await();
        TestCase.assertTrue(helper.getClient().getChildren("/hellowork").size() == count);
        helper.deleteNode("/hellowork");
        TestCase.assertTrue(!helper.getClient().exists("/hellowork"));
        helper.destroy();
    }
}
