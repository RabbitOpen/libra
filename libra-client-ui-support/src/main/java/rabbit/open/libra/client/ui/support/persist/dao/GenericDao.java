package rabbit.open.libra.client.ui.support.persist.dao;

import rabbit.open.orm.core.dml.SessionFactory;
import rabbit.open.orm.core.spring.SpringDaoAdapter;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

/**
 * 通用Dao
 * @author xiaoqianbin
 * @date 2020/7/28
 **/
public abstract class GenericDao<T> extends SpringDaoAdapter<T> {

    @Resource
    private SessionFactory factory;

    @PostConstruct
    public void setup() {
        setSessionFactory(factory);
    }
}
