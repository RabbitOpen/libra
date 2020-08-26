package rabbit.open.libra.ui.support.persist.entity;

import rabbit.open.orm.common.dml.Policy;
import rabbit.open.orm.core.annotation.Column;
import rabbit.open.orm.core.annotation.PrimaryKey;

import java.util.Date;

/**
 * 执行记录
 * @author xiaoqianbin
 * @date 2020/8/26
 **/
public class ExecutionRecord {

    @PrimaryKey(policy = Policy.UUID)
    @Column(value = "id", length = 40)
    private String id;

    /**
     * 启动日期
     **/
    @Column(value = "start", keyWord = true)
    private Date start;

    /**
     * 结束日期
     **/
    @Column(value = "end", keyWord = true)
    private Date end;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Date getStart() {
        return start;
    }

    public void setStart(Date start) {
        this.start = start;
    }

    public Date getEnd() {
        return end;
    }

    public void setEnd(Date end) {
        this.end = end;
    }
}
