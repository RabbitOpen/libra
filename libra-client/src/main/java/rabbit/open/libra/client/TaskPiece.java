package rabbit.open.libra.client;

/**
 * 抽象分片任务
 * @author xiaoqianbin
 * @date 2020/7/10
 **/
public abstract class TaskPiece {

    /**
     * 默认 app name
     **/
    public static final String DEFAULT_APP = "default-app";

    /**
     * 默认分组
     **/
    public static final String DEFAULT_GROUP = "DEFAULT";

    /**
     * 默认执行顺序
     **/
    public static final int DEFAULT_EXECUTOR_ORDER = 0;

    /**
     * 默认分片数
     **/
    public static final int DEFAULT_SPLIT_COUNT = 1;

    /**
     * 默认并发度
     **/
    public static final int DEFAULT_PARALLEL = 1;

    /**
     * 任务组 SYSTEM 任务组的任务一旦注册就会被自动启动
     * @author  xiaoqianbin
     * @date    2020/7/10
     **/
    public String getTaskGroup() {
        return DEFAULT_GROUP;
    }

    /**
     * 任务名
     * @author  xiaoqianbin
     * @date    2020/7/10
     **/
    public String getTaskName() {
        return getClass().getSimpleName();
    }

    /**
     * 组内任务执行顺序，小的优先
     * @author  xiaoqianbin
     * @date    2020/7/10
     **/
    protected Integer getExecuteOrder() {
        return DEFAULT_EXECUTOR_ORDER;
    }

    /**
     * 任务切片数
     * @author  xiaoqianbin
     * @date    2020/7/10
     **/
    protected int getSplitsCount() {
        return DEFAULT_SPLIT_COUNT;
    }

    /**
     * 任务最大并行度
     * @author  xiaoqianbin
     * @date    2020/7/10
     **/
    protected int getParallel() {
        return DEFAULT_PARALLEL;
    }

    /**
     * 执行任务
     * @param	index           分片任务id
	 * @param	splits          任务的总并发度
	 * @param	taskScheduleTime     yyyyMMddHHmmss
     * @author  xiaoqianbin
     * @date    2020/7/10
     **/
    public abstract void execute(int index, int splits, String taskScheduleTime);

    /**
     * 关闭任务
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    protected abstract void close();

    /**
     * 任务执行周期
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    protected abstract String getCronExpression();

    /**
     * 应用名
     * @author  xiaoqianbin
     * @date    2020/7/16
     **/
    public String getAppName() {
        return DEFAULT_APP;
    }
}
