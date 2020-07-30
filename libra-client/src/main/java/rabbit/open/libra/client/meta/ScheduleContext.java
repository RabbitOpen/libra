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

    // 上下文数据
    private Serializable contextData;

    // 切片数
    private int splitCount;

    // 调度id(一组下的调度 调度id相同)
    private String scheduleId;

    public ScheduleContext() {
    }

    public ScheduleContext(Date scheduleFireDate, int index, int splitCount, String scheduleId) {
        this.scheduleFireDate = scheduleFireDate;
        this.index = index;
        this.splitCount = splitCount;
        this.scheduleId = scheduleId;
    }

    public Date getScheduleFireDate() {
        return scheduleFireDate;
    }

    public int getIndex() {
        return index;
    }

    public int getSplitCount() {
        return splitCount;
    }

    public String getScheduleId() {
        return scheduleId;
    }

    public Serializable getContextData() {
        return contextData;
    }
}
