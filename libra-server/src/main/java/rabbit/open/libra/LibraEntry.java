package rabbit.open.libra;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;
import rabbit.open.libra.client.annotation.EnableLibraWebUI;

/**
 * 调度服务端入口程序
 * @author xiaoqianbin
 * @date 2020/7/10
 **/
@SpringBootApplication
@ImportResource(locations = {"classpath*:applicationContext.xml"})
@EnableLibraWebUI
public class LibraEntry {

    public static void main(String[] args) {
        SpringApplication.run(LibraEntry.class);
    }
}
