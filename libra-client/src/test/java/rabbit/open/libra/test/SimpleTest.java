package rabbit.open.libra.test;

import junit.framework.TestCase;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import rabbit.open.libra.client.AbstractLibraTask;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.TaskMeta;
import rabbit.open.libra.client.task.DistributedTask;
import rabbit.open.libra.client.task.SchedulerTask;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 任务测试
 * @author xiaoqianbin
 * @date 2020/7/24
 **/
@RunWith(JUnit4.class)
public class SimpleTest {

    /**
     * 编组测试
     * @author  xiaoqianbin
     * @date    2020/7/24
     **/
    @Test
    public void groupTest() throws Exception {
        clearMap();
        RegistryHelper registryHelper = getHelper();
        Semaphore holdOn = new Semaphore(0);
        MySchedulerTask st = new MySchedulerTask(registryHelper) {
            @Override
            protected void loadTaskMetas() {
                super.loadTaskMetas();
                holdOn.release();
            }
        };
        st.afterPropertiesSet();
        T1 t1 = new T1(registryHelper);
        t1.afterPropertiesSet();
        T2 t2 = new T2(registryHelper);
        t2.afterPropertiesSet();
        T3 t3 = new T3(registryHelper);
        t3.afterPropertiesSet();
        T4 t4 = new T4(registryHelper);
        t4.afterPropertiesSet();
        AbstractLibraTask.runScheduleTasks();
        holdOn.acquire();
        List<TaskMeta> gt1 = st.getTaskMetas().get(t1.getAppName()).get(t1.getTaskGroup());
        List<TaskMeta> gt3 = st.getTaskMetas().get(t1.getAppName()).get(t3.getTaskGroup());
        List<TaskMeta> gt4 = st.getTaskMetas().get(t1.getAppName()).get(t4.getTaskGroup());
        TestCase.assertEquals(2, gt1.size());
        TestCase.assertEquals(1, gt3.size());
        TestCase.assertEquals(1, gt4.size());
        TestCase.assertEquals(t1.getTaskName(), gt1.get(0).getTaskName());
        TestCase.assertEquals(t2.getTaskName(), gt1.get(1).getTaskName());
        AbstractLibraTask.shutdown();
    }

    /**
     * 清理全局静态变量
     * @author  xiaoqianbin
     * @date    2020/7/24
     **/
    private void clearMap() throws NoSuchFieldException, IllegalAccessException {
        Field taskMetaCache = AbstractLibraTask.class.getDeclaredField("taskMetaCache");
        taskMetaCache.setAccessible(true);
        @SuppressWarnings("unchecked")
		Map<String, Map<String, List<TaskMeta>>> map = (Map<String, Map<String, List<TaskMeta>>>) taskMetaCache.get(null);
        map.clear();
    }

    private RegistryHelper getHelper() {
        RegistryHelper registryHelper = new RegistryHelper();
        registryHelper.setRootPath("/libra/simple-test");
        registryHelper.setHosts("localhost:2181");
        ZkClient zkClient = new ZkClient("localhost:2181");
        zkClient.deleteRecursive("/libra/simple-test");
        zkClient.close();
        registryHelper.init();
        return registryHelper;
    }

    private AtomicLong counter;
    /**
     * 调度测试
     * @author  xiaoqianbin
     * @date    2020/7/24
     **/
    @Test
    public void scheduleTest() throws Exception {
        clearMap();
        RegistryHelper registryHelper = getHelper();
        Semaphore holdOn = new Semaphore(0);
        Semaphore step = new Semaphore(0);
        counter = new AtomicLong(10);
        MySchedulerTask st = new MySchedulerTask(registryHelper){
            @Override
            protected void loadTaskMetas() {
                super.loadTaskMetas();
                holdOn.release();
            }

            @Override
            protected void prePublish(String appName, String group, String taskName, String scheduleTime) {
                logger.info("{}-{}-{}", appName, group, taskName);
                if ("GTS-T2".equals(taskName)) {
                    TestCase.assertEquals(16, counter.get());
                } else if ("GTS-T3".equals(taskName)) {
                    TestCase.assertEquals(22, counter.get());
                }
            }

            @Override
            protected void taskFinished(String appName, String group, String taskName, String scheduleTime) {
                if ("GTS-T3".equals(taskName)) {
                	logger.info("whole group[{}] is finished", group);
                	step.release();
                }
            }
        };
        st.afterPropertiesSet();
        T1 t1 = new T1(registryHelper) {

            @Override
            public String getTaskGroup() {
                return "GTS";
            }

            @Override
            public String getTaskName() {
                return "GTS-T1";
            }

            @Override
            protected Integer getExecuteOrder() {
                return 0;
            }

            @Override
            protected int getSplitsCount() {
                return 2;
            }

            @Override
            protected String getCronExpression() {
                return "0/20 * * * * *";
            }

            @Override
            public void execute(int index, int splits, String taskScheduleTime) {
                counter.addAndGet(3);
            }
        };
        t1.afterPropertiesSet();
        T2 t2 = new T2(registryHelper) {

            @Override
            public String getTaskGroup() {
                return "GTS";
            }

            @Override
            public String getTaskName() {
                return "GTS-T2";
            }

            @Override
            protected Integer getExecuteOrder() {
                return 1;
            }

            @Override
            protected int getSplitsCount() {
                return 2;
            }

            @Override
            protected String getCronExpression() {
                return "0/20 * * * * *";
            }

            @Override
            public void execute(int index, int splits, String taskScheduleTime) {
                counter.addAndGet(3);
            }
        };
        t2.afterPropertiesSet();
        T3 t3 = new T3(registryHelper) {

            @Override
            public String getTaskGroup() {
                return "GTS";
            }

            @Override
            public String getTaskName() {
                return "GTS-T3";
            }

            @Override
            protected Integer getExecuteOrder() {
                return 2;
            }

            @Override
            protected String getCronExpression() {
                return "0/20 * * * * *";
            }

            @Override
            public void execute(int index, int splits, String taskScheduleTime) {
                counter.addAndGet(10);
            }
        };
        t3.afterPropertiesSet();
        AbstractLibraTask.runScheduleTasks();
        holdOn.acquire();
        List<TaskMeta> gt1 = st.getTaskMetas().get(t1.getAppName()).get(t1.getTaskGroup());
        TestCase.assertEquals(3, gt1.size());
        TestCase.assertEquals(t1.getTaskName(), gt1.get(0).getTaskName());
        TestCase.assertEquals(t2.getTaskName(), gt1.get(1).getTaskName());
        TestCase.assertEquals(t3.getTaskName(), gt1.get(2).getTaskName());
        while (true) {
            if (step.tryAcquire(3, TimeUnit.SECONDS)) {
                TestCase.assertEquals(32, counter.get());
                break;
            }
        }
        AbstractLibraTask.shutdown();
    }

    /**
     * 手工调度测试
     * @author  xiaoqianbin
     * @date    2020/7/24
     **/
    @Test
    public void manualTest() throws Exception {
        clearMap();
        RegistryHelper registryHelper = getHelper();
        Semaphore holdOn = new Semaphore(0);
        Semaphore step = new Semaphore(0);
        counter = new AtomicLong(10);
        MySchedulerTask st = new MySchedulerTask(registryHelper){
            @Override
            protected void loadTaskMetas() {
                super.loadTaskMetas();
                holdOn.release();
            }

            @Override
            protected void prePublish(String appName, String group, String taskName, String scheduleTime) {

            }

            @Override
            protected void taskFinished(String appName, String group, String taskName, String scheduleTime) {
                logger.info("task [{}-{}-{}] is finished", group, taskName, scheduleTime);
                step.release();
            }
        };
        st.afterPropertiesSet();
        T1 t1 = new T1(registryHelper) {

            @Override
            public String getTaskGroup() {
                return "GTS";
            }

            @Override
            public String getTaskName() {
                return "GTS-T1";
            }

            @Override
            protected Integer getExecuteOrder() {
                return 0;
            }

            @Override
            protected int getSplitsCount() {
                return 2;
            }

            @Override
            protected String getCronExpression() {
                return "0 0 2 * * *";
            }

            @Override
            public void execute(int index, int splits, String taskScheduleTime) {
                counter.addAndGet(3);
            }
        };
        t1.afterPropertiesSet();
        T2 t2 = new T2(registryHelper) {

            @Override
            public String getTaskGroup() {
                return "GTS";
            }

            @Override
            public String getTaskName() {
                return "GTS-T2";
            }

            @Override
            protected Integer getExecuteOrder() {
                return 1;
            }

            @Override
            protected int getSplitsCount() {
                return 2;
            }

            @Override
            protected String getCronExpression() {
                return "0 0 2 * * *";
            }

            @Override
            public void execute(int index, int splits, String taskScheduleTime) {
                counter.addAndGet(3);
            }
        };
        t2.afterPropertiesSet();
        T3 t3 = new T3(registryHelper) {

            @Override
            public String getTaskGroup() {
                return "GTS";
            }

            @Override
            public String getTaskName() {
                return "GTS-T3";
            }

            @Override
            protected Integer getExecuteOrder() {
                return 2;
            }

            @Override
            protected String getCronExpression() {
                return "0 0 2 * * *";
            }

            @Override
            public void execute(int index, int splits, String taskScheduleTime) {
                counter.addAndGet(10);
            }
        };
        t3.afterPropertiesSet();
        AbstractLibraTask.runScheduleTasks();
        holdOn.acquire();
        List<TaskMeta> gt1 = st.getTaskMetas().get(t1.getAppName()).get(t1.getTaskGroup());
        TestCase.assertEquals(3, gt1.size());
        TestCase.assertEquals(t1.getTaskName(), gt1.get(0).getTaskName());
        TestCase.assertEquals(t2.getTaskName(), gt1.get(1).getTaskName());
        TestCase.assertEquals(t3.getTaskName(), gt1.get(2).getTaskName());

        // 发布t2
        t1.publishTask(t1.getAppName(), t1.getTaskGroup(), t2.getTaskName(), "20200722", false);
        step.acquire();
        TestCase.assertEquals(0, step.availablePermits());
        TestCase.assertEquals(16, counter.get());
        t1.publishTask(t1.getAppName(), t1.getTaskGroup(), t2.getTaskName(), "20200723", true);
        step.acquire(2);
       
        TestCase.assertEquals(0, step.availablePermits());
        TestCase.assertEquals(32, counter.get());
        AbstractLibraTask.shutdown();

    }


    public static class MySchedulerTask extends SchedulerTask {

        public MySchedulerTask(RegistryHelper helper) {
            this.helper = helper;
        }

        @Override
        public RegistryHelper getRegistryHelper() {

            return helper;
        }

        public Map<String, Map<String, List<TaskMeta>>> getTaskMetas() {
            return taskMetaMap;
        }
    }

    public static class T1 extends DistributedTask {

        private RegistryHelper helper;

        public T1(RegistryHelper helper) {
            this.helper = helper;
        }

        @Override
        public RegistryHelper getRegistryHelper() {
            return helper;
        }

        @Override
        public void execute(int index, int splits, String taskScheduleTime) {

        }

        @Override
        protected String getCronExpression() {
            return "0/5 * * * * *";
        }

        @Override
        public String getTaskGroup() {
            return "g1";
        }

    }

    public static class T2 extends DistributedTask {

        private RegistryHelper helper;

        public T2(RegistryHelper helper) {
            this.helper = helper;
        }

        @Override
        public RegistryHelper getRegistryHelper() {
            return helper;
        }

        @Override
        public void execute(int index, int splits, String taskScheduleTime) {

        }

        @Override
        protected Integer getExecuteOrder() {
            return 1;
        }

        @Override
        protected String getCronExpression() {
            return "0/5 * * * * *";
        }

        @Override
        public String getTaskGroup() {
            return "g1";
        }
    }

    public static class T3 extends DistributedTask {

        private RegistryHelper helper;

        public T3(RegistryHelper helper) {
            this.helper = helper;
        }

        @Override
        public RegistryHelper getRegistryHelper() {
            return helper;
        }

        @Override
        public void execute(int index, int splits, String taskScheduleTime) {

        }

        @Override
        protected String getCronExpression() {
            return "0/5 * * * * *";
        }

        @Override
        public String getTaskGroup() {
            return "g3";
        }
    }

    public static class T4 extends DistributedTask {

        private RegistryHelper helper;

        public T4(RegistryHelper helper) {
            this.helper = helper;
        }

        @Override
        public RegistryHelper getRegistryHelper() {
            return helper;
        }

        @Override
        public void execute(int index, int splits, String taskScheduleTime) {

        }

        @Override
        protected String getCronExpression() {
            return "0/5 * * * * *";
        }

        @Override
        public String getTaskGroup() {
            return "g2";
        }
    }
}



