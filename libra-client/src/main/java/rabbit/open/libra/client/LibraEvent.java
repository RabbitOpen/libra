package rabbit.open.libra.client;

/**
 * libra事件
 * @author xiaoqianbin
 * @date 2020/7/28
 **/
public interface LibraEvent {

    /**
     * 任务完成了
     * @param	appName
     * @param	group
     * @param	taskName
     * @param	scheduleTime
     * @author  xiaoqianbin
     * @date    2020/7/20
     **/
    default void onTaskCompleted(String appName, String group, String taskName, String scheduleTime) {
        // TO DO: record this execution
    }

    /**
     * 任务开始了
     * @param	appName
     * @param	group
     * @param	taskName
     * @param	scheduleTime
     * @author  xiaoqianbin
     * @date    2020/7/20
     **/
    default void onTaskStarted(String appName, String group, String taskName, String scheduleTime) {
        // TO DO: record this execution
    }
}
