package rabbit.open.libra.client;

/**
 * 常量
 * @author xiaoqianbin
 * @date 2020/8/17
 **/
public abstract class Constant {

    private Constant() {}

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
}
