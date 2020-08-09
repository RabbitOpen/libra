package rabbit.open.libra.dag.exception;

/**
 * dag无路径异常
 * @author xiaoqianbin
 * @date 2020/8/8
 **/
@SuppressWarnings("serial")
public class NoPathException extends DagException {

    public NoPathException() {
        super("no path is found exception");
    }
}
