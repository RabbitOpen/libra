package rabbit.open.libra.test;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.data.Stat;

/**
 * @author xiaoqianbin
 * @date 2020/8/20
 **/
public class ZkTest {

    public static void main(String[] args) {

        ZkClient client = new ZkClient("localhost:2181");

        client.writeData("/xd", "hello");
        Stat stat = new Stat();
        System.out.println(stat.getVersion());
        client.readData("/xd", stat);
        client.writeData("/xd", "hello", stat.getVersion());
        System.out.println(stat.getVersion());


    }
}
