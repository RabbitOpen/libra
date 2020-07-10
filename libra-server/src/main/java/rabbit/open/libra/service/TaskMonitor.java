package rabbit.open.libra.service;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * 分布式任务监控器
 * @author xiaoqianbin
 * @date 2020/7/10
 **/
@Service
public class TaskMonitor {

    private Logger logger = LoggerFactory.getLogger(getClass());

    // zk地址
    @Value("${zookeeper.hosts.url}")
    private String hosts;

    // 监控根节点
    @Value("${libra.monitor.root-path:/libra/root}")
    private String rootPath;

    private ZkClient client;

    @PostConstruct
    public void init() {
        client = new ZkClient(hosts);
    }

    @PreDestroy
    public void destroy() {
        client.close();
    }
}
