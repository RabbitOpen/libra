package rabbit.open.test;

import org.I0Itec.zkclient.ZkClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import rabbit.open.libra.LibraEntry;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.task.DistributedTask;
import rabbit.open.libra.client.task.WebSchedulerTask;

import java.sql.Time;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * WebSchedulerTask  单元测试
 * @author xiaoqianbin
 * @date 2020/7/28
 **/
@RunWith(SpringRunner.class)
@SpringBootTest(classes = LibraEntry.class)
public class WebSchedulerTest {

    private static Semaphore semaphore;

    @Autowired
    private RegistryHelper helper;

    @Autowired
    WebSchedulerTask webSchedulerTask;

    @Test
    public void simpleJobTest() throws Exception {
        SimpleTask task = new SimpleTask();
        task.setRegistryHelper(helper);
        task.afterPropertiesSet();
        semaphore = new Semaphore(0);
        semaphore.acquire();
    }

    public static class SimpleTask extends DistributedTask {

        private RegistryHelper registryHelper;

        private Semaphore hold = new Semaphore(0);

        public void setRegistryHelper(RegistryHelper registryHelper) {
            this.registryHelper = registryHelper;
        }

        @Override
        public RegistryHelper getRegistryHelper() {
            return registryHelper;
        }

        @Override
        public void execute(int index, int splits, String taskScheduleTime) {
            logger.info("run {}", getTaskName());
            try {
                hold.tryAcquire(2, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        @Override
        public void onTaskCompleted(String appName, String group, String taskName, String scheduleTime) {
            try {
                hold.tryAcquire(2, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            semaphore.release();
        }

        @Override
        protected String getCronExpression() {
            return "0/5 * * * * *";
        }
    }
}
