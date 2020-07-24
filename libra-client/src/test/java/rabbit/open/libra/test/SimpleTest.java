package rabbit.open.libra.test;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import rabbit.open.libra.client.AbstractLibraTask;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.TaskMeta;
import rabbit.open.libra.client.task.DistributedTask;
import rabbit.open.libra.client.task.SchedulerTask;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * 任务测试
 * @author xiaoqianbin
 * @date 2020/7/24
 **/
@RunWith(JUnit4.class)
public class SimpleTest {

    static RegistryHelper registryHelper = new RegistryHelper();
    /**
     * 编组测试
     * @author  xiaoqianbin
     * @date    2020/7/24
     **/
    @Test
    public void groupTest() throws Exception {
        Semaphore s = new Semaphore(0);
        MySchedulerTask st = new MySchedulerTask(registryHelper) {
            @Override
            protected void loadTaskMetas() {
                super.loadTaskMetas();
                s.release();
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
        s.acquire();
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



    static {
        registryHelper.setRootPath("/libra/simple-test");
        registryHelper.setHosts("localhost:2181");
        registryHelper.init();
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



