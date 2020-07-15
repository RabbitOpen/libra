package rabbit.open.test;

import org.I0Itec.zkclient.ZkClient;

import java.io.IOException;

/**
 * @author xiaoqianbin
 * @date 2020/7/15
 **/
public class ZkTest {

    public static void main(String[] args) throws IOException {

        ZkClient client = new ZkClient("localhost:2181");

        client.subscribeChildChanges("/hello", (p, list) -> {

            System.out.println("/hello  changed, " + list);
            Thread.sleep(10000);

        });
        System.in.read();
        client.close();
    }
}
