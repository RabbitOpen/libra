package rabbit.open.libra.client;

/**
 * 抽象分片任务
 * @author xiaoqianbin
 * @date 2020/7/10
 **/
public abstract class TaskPiece {

    /**
     * 任务组 SYSTEM 任务组的任务一旦注册就会被自动启动
     * @author  xiaoqianbin
     * @date    2020/7/10
     **/
    protected String getTaskGroup() {
        return "DEFAULT";
    }

    /**
     * 任务名
     * @author  xiaoqianbin
     * @date    2020/7/10
     **/
    protected String getTaskName() {
        return getClass().getSimpleName();
    }

    /**
     * 组内任务执行顺序，小的优先
     * @author  xiaoqianbin
     * @date    2020/7/10
     **/
    protected Integer getExecuteOrder() {
        return 0;
    }

    /**
     * 任务切片数
     * @author  xiaoqianbin
     * @date    2020/7/10
     **/
    protected int getSplitsCount() {
        return 1;
    }

    /**
     * 任务最大并行度
     * @author  xiaoqianbin
     * @date    2020/7/10
     **/
    protected int getParallel() {
        return 1;
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
}
