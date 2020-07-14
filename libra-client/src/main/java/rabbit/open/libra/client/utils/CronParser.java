package rabbit.open.libra.client.utils;

import org.springframework.scheduling.TriggerContext;
import org.springframework.scheduling.support.CronTrigger;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * cron表达式解析器
 * @author xiaoqianbin
 * @date 2020/7/14
 **/
public class CronParser {

    public static void main(String[] args) {

        CronTrigger trigger = new CronTrigger("0 0 03,22,23 * * *");
        Date date = trigger.nextExecutionTime(new TriggerContext() {

            @Override
            public Date lastScheduledExecutionTime() {
                return null;
            }

            @Override
            public Date lastActualExecutionTime() {
                return null;
            }

            @Override
            public Date lastCompletionTime() {
                return null;
            }
        });
        System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date));
    }
}
