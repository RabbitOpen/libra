package rabbit.open.libra.client;

/**
 * 调度类型
 * @author xiaoqianbin
 * @date 2020/7/24
 **/
public enum ScheduleType {

    /**
     * 定时自动调度
     **/
    AUTO,

    /**
     * 人工按组调度
     **/
    MANUAL_GROUP,

    /**
     * 人工单独调度
     **/
    MANUAL_SINGLE

}
