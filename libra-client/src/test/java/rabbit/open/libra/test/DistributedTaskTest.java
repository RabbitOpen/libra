package rabbit.open.libra.test;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;

/**
 * @author xiaoqianbin
 * @date 2020/8/14
 **/
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"classpath*:/applicationContext.xml"})
public class DistributedTaskTest {

    @Test
    public void t1() throws IOException {

        System.in.read();
    }

}
