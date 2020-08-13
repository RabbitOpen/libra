package rabbit.open.libra.test;

import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import rabbit.open.libra.client.RegistryHelper;

/**
 * 单元测试
 * @author xiaoqianbin
 * @date 2020/8/11
 **/
@RunWith(JUnit4.class)
public class RegistryTest {

    @Test
    public void createNodeTest() {
        init("/megrez/loan");
        RegistryHelper helper = new RegistryHelper();
        helper.setNamespace("/megrez/loan");
        helper.setHosts("localhost:2181");
        helper.init();
        helper.destroy();
    }

    private void init(String path) {
        ZkClient client = new ZkClient("localhost:2181");
        client.deleteRecursive(path);
        client.close();
    }
}
