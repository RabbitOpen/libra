package rabbit.open.libra.client.exception;

/**
 * libra 基础异常
 * @author xiaoqianbin
 * @date 2020/7/16
 **/
@SuppressWarnings("serial")
public class LibraException extends RuntimeException {

    public LibraException(String message) {
        super(message);
    }
}
