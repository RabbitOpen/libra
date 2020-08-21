package rabbit.open.libra.client.dag;

/**
 * 版本化的数据
 * @author  xiaoqianbin
 * @date    2020/8/20
 **/
public interface VersionedData {

    int getVersion();

    void setVersion(int version);
}
