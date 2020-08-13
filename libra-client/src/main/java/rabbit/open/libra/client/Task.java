package rabbit.open.libra.client;

import rabbit.open.libra.dag.schedule.ScheduleContext;

/**
 * 分布式任务模型
 * @author xiaoqianbin
 * @date 2020/8/11
 **/
public abstract class Task {

    /**
     * 默认切片数
     **/
    public static final int DEFAULT_SPLITS_COUNT = 1;

    /**
     * 默认任务并发数
     **/
    public static final int DEFAULT_CONCURRENCE_COUNT = 3;

    /**
     * 分隔符
     **/
    public static final String SP = "/";

    /**
     * 任务名
     * @author  xiaoqianbin
     * @date    2020/8/11
     **/
    public String getTaskName() {
        return getClass().getSimpleName();
    }

    /**
     * 执行任务
     * @param	context
     * @author  xiaoqianbin
     * @date    2020/8/11
     **/
    public abstract void execute(ScheduleContext context);

    /**
     * 应用名
     * @author  xiaoqianbin
     * @date    2020/8/11
     **/
    public String getAppName() {
        return "libra";
    }

    /**
     * 任务分片数
     * @author  xiaoqianbin
     * @date    2020/8/11
     **/
    public int getSplitsCount() {
        return DEFAULT_SPLITS_COUNT;
    }

    /**
     * 分片最大并发处理数
     * @author  xiaoqianbin
     * @date    2020/8/11
     **/
    public int getConcurrenceCount() {
        return DEFAULT_CONCURRENCE_COUNT;
    }
}
