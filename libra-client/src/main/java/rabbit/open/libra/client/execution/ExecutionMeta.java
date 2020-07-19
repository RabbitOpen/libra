package rabbit.open.libra.client.execution;

import java.io.Serializable;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 任务运行信息
 * @author xiaoqianbin
 * @date 2020/7/13
 **/
@SuppressWarnings("serial")
public class ExecutionMeta implements Serializable {

    private String start;

    private String end;

    private String taskName;

    private String executor;

    /**
     * 构造函数
     * @param	start
	 * @param	end
	 * @param	taskName
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    public ExecutionMeta(Date start, Date end, String taskName) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        setStart(formatter.format(start));
        if (null != end) {
            setEnd(formatter.format(end));
        }
        setTaskName(taskName);
        try {
            setExecutor(InetAddress.getLocalHost().getHostName());
        } catch (Exception e) {
            // TO DO: ignore
        }
    }

    public String getExecutor() {
        return executor;
    }

    public void setExecutor(String executor) {
        this.executor = executor;
    }

    public String getStart() {
        return start;
    }

    public void setStart(String start) {
        this.start = start;
    }

    public String getEnd() {
        return end;
    }

    public void setEnd(String end) {
        this.end = end;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }
}
