package rabbit.open.libra.client.exception;

/**
 * <b>@description 重复调度异常 </b>
 */
@SuppressWarnings("serial")
public class RepeatedScheduleException extends LibraException {

	public RepeatedScheduleException(String dagId) {
		super(String.format("repeated schedule on dag[%s]", dagId));
	}

}
