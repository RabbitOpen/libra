package rabbit.open.libra.client;

import org.springframework.beans.factory.annotation.Value;

import java.io.Serializable;

/**
 * 注册中心配置对象
 * @author xiaoqianbin
 * @date 2020/8/16
 **/
@SuppressWarnings("serial")
public class RegistryConfig implements Serializable {

    // zk地址
    @Value("${zookeeper.hosts.url:localhost:2181}")
    private String hosts;

    // 监控根节点
    @Value("${libra.monitor.namespace:/libra/root}")
    private String namespace;

    public String getHosts() {
        return hosts;
    }

    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
}
