package rabbit.open.libra.ui.support.persist;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import rabbit.open.libra.ui.support.persist.entity.TaskExecutionRecord;

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

    public static void main(String[] args) {
        int length = TaskExecutionRecord.class.getSimpleName().length() + 1;
        System.out.println(TaskExecutionRecord.class.getName().substring(0,
                TaskExecutionRecord.class.getName().length() - length));
    }
}
