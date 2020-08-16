package rabbit.open.libra.test;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import rabbit.open.libra.client.RegistryHelper;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 单元测试
 * @author xiaoqianbin
 * @date 2020/8/11
 **/
@RunWith(JUnit4.class)
public class RegistryTest {

    @Test
    public void createNodeTest() throws InterruptedException {
        init("/libra/loan");
        RegistryHelper helper = new RegistryHelper("localhost:2181", "/libra/loan");
        helper.init();
        Semaphore s = new Semaphore(0);
        helper.createNode("/xx");
        helper.subscribeDataChanges("/xx", new IZkDataListener() {

            @Override
            public void handleDataChange(String s, Object o) throws Exception {
                System.out.println(o.toString());
                Thread.sleep(2000);
            }

            @Override
            public void handleDataDeleted(String s) throws Exception {

            }

        });
        s.tryAcquire(1, TimeUnit.SECONDS);
        helper.writeData("/xx", "hello");
        s.tryAcquire(1, TimeUnit.SECONDS);
        helper.writeData("/xx", "go1");
        helper.writeData("/xx", "go2");
        helper.writeData("/xx", "go3");
        helper.writeData("/xx", "go4");
        helper.writeData("/xx", "go5");
        helper.writeData("/xx", "go6");
        s.tryAcquire(1, TimeUnit.SECONDS);
        helper.destroy();
    }

    private void init(String path) {
        ZkClient client = new ZkClient("localhost:2181");
        client.deleteRecursive(path);
        client.close();
    }
}
