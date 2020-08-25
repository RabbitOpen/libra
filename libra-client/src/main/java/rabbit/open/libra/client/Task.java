package rabbit.open.libra.client;

import rabbit.open.libra.dag.schedule.ScheduleContext;

/**
 * 分布式任务模型
 * @author xiaoqianbin
 * @date 2020/8/11
 **/
public interface Task {

    /**
     * 任务名
     * @author xiaoqianbin
     * @date 2020/8/11
     **/
    default String getTaskName() {
        return getClass().getSimpleName();
    }

    /**
     * 执行任务
     * @param    context
     * @author xiaoqianbin
     * @date 2020/8/11
     **/
    void execute(ScheduleContext context);

    /**
     * 应用名
     * @author xiaoqianbin
     * @date 2020/8/11
     **/
    default String getAppName() {
        return "libra";
    }

    /**
     * 任务分片数
     * @author xiaoqianbin
     * @date 2020/8/11
     **/
    default int getSplitsCount() {
        return Constant.DEFAULT_SPLITS_COUNT;
    }

    /**
     * 分片最大并发处理数
     * @author xiaoqianbin
     * @date 2020/8/11
     **/
    default int getParallel() {
        return Constant.DEFAULT_PARALLEL_COUNT;
    }
}
