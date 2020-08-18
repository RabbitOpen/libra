package rabbit.open.libra.client.meta;

import java.io.Serializable;
import java.util.Date;

/**
 * dag 元信息
 * @author xiaoqianbin
 * @date 2020/8/18
 **/
@SuppressWarnings("serial")
public class DagMeta implements Serializable {

    // dag name
    private String dagName;

    // dag id信息
    private String dagId;

    // 调度周期表达式
    private String cronExpression;

    // 上一个调度日期 yyyy-MM-dd HH:mm:ss
    private Date lastFireDate;

    public DagMeta(String dagName, String dagId, String cronExpression) {
        this.dagName = dagName;
        this.dagId = dagId;
        this.cronExpression = cronExpression;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    public Date getLastFireDate() {
        return lastFireDate;
    }

    public void setLastFireDate(Date lastFireDate) {
        this.lastFireDate = lastFireDate;
    }

    public String getDagName() {
        return dagName;
    }

    public void setDagName(String dagName) {
        this.dagName = dagName;
    }

    public String getDagId() {
        return dagId;
    }

    public void setDagId(String dagId) {
        this.dagId = dagId;
    }
}
