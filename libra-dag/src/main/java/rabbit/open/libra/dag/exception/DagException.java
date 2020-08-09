package rabbit.open.libra.dag.exception;

/**
 * dag exception基类
 * @author xiaoqianbin
 * @date 2020/8/8
 **/
@SuppressWarnings("serial")
public class DagException extends RuntimeException {

    public DagException(String message) {
        super(message);
    }
}
