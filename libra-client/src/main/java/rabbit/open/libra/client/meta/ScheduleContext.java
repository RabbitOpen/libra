package rabbit.open.libra.client.meta;

import java.io.Serializable;
import java.util.Date;

/**
 * 调度上下文
 * @author xiaoqianbin
 * @date 2020/7/30
 **/
@SuppressWarnings("serial")
public class ScheduleContext implements Serializable {

    // 调度触发日期
    private Date scheduleFireDate;

    // 切片index
    private int index;

    // 切片数
    private int splitCount;

    // 调度id(一组下的调度 调度id相同)
    private String scheduleId;
}
