package rabbit.open.libra.client.ui.support.persist;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * libra web UI controller
 * @author xiaoqianbin
 * @date 2020/7/28
 **/
@RequestMapping("/libra")
public class LibraController {

    @ResponseBody
    @RequestMapping("/hello")
    public String hello() {
        return "hello";
    }

    @RequestMapping("/portal")
    public String portal() {
        return "portal";
    }
}
