package rabbit.open.libra.dag.exception;

/**
 * dag循环异常
 * @author xiaoqianbin
 * @date 2020/8/8
 **/
public class CyclicDagException extends DagException {

    public CyclicDagException(String message) {
        super(message);
    }
}
