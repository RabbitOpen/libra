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

    /**
     * 发布任务前
     * @param	appName
     * @param	group
     * @param	taskName
     * @param	scheduleTime
     * @author  xiaoqianbin
     * @date    2020/7/19
     **/
    default void prePublish(String appName, String group, String taskName, String scheduleTime) {
        // TO DO: record this publish
    }

    /**
     * 发布任务后
     * @param	appName
     * @param	group
     * @param	taskName
     * @param	scheduleTime
     * @author  xiaoqianbin
     * @date    2020/7/19
     **/
    default void postPublish(String appName, String group, String taskName, String scheduleTime) {
        // TO DO: record this publish
    }
}
