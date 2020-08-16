package rabbit.open.libra.client.task;

import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;
import rabbit.open.libra.client.RegistryConfig;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.Task;
import rabbit.open.libra.client.ZookeeperMonitor;
import rabbit.open.libra.client.meta.TaskMeta;
import rabbit.open.libra.dag.schedule.ScheduleContext;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 调度任务
 * @author xiaoqianbin
 * @date 2020/8/16
 **/
public class SchedulerTask extends ZookeeperMonitor implements Task {

    @Autowired
    RegistryConfig config;

    /**
     * 标识节点是否就是leader
     **/
    protected boolean leader = false;

    /**
     * 调度线程
     **/
    private Thread schedulerThread;

    /**
     * 退出调度信号
     **/
    private Semaphore quitSemaphore = new Semaphore(0);

    @PostConstruct
    @Override
    public void init() {
        super.init();
        registerTaskMeta();
        execute(null);
    }

    @Override
    public RegistryConfig getConfig() {
        return config;
    }

    @Override
    public void execute(ScheduleContext context) {
        String schedulePath = RegistryHelper.META_SCHEDULER + SP + getTaskName();
        getRegistryHelper().subscribeChildChanges(schedulePath, (path, list) -> {
            if (!list.contains(getTaskName())) {
                logger.info("leader is lost");
                try2AcquireControl(schedulePath, getLeaderName(), CreateMode.EPHEMERAL);
            } else {
                if (getLeaderName().equals(getRegistryHelper().readData(schedulePath))) {
                    leader = true;
                } else {
                    leader = false;
                }
            }
        });
        try2AcquireControl(schedulePath, getLeaderName(), CreateMode.EPHEMERAL);
        startScheduleThread();
    }

    /**
     * 启动调度线程
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    private void startScheduleThread() {
        schedulerThread = new Thread(() -> {
            while (true) {
                try {
                    if (quitSemaphore.tryAcquire(3, TimeUnit.SECONDS)) {
                        break;
                    }
                    if (leader) {
//                        beginSchedule();
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
            logger.info("scheduler thread is exited....");
        }, getTaskName());
        schedulerThread.setDaemon(false);
        schedulerThread.start();
    }

    /**
     * 尝试获取控制权
     * @param path
     * @param data
     * @param mode
     * @author xiaoqianbin
     * @date 2020/7/13
     **/
    protected boolean try2AcquireControl(String path, Object data, CreateMode mode) {
        try {
            getRegistryHelper().create(path, data, mode);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    protected void onZookeeperDisconnected() {

    }

    @Override
    protected void onZookeeperConnected() {

    }

    /**
     * 应用关闭
     * @author  xiaoqianbin
     * @date    2020/8/16
     **/
    @PreDestroy
    public void destroy() {
        quitSemaphore.release();
        try {
            schedulerThread.join();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        getRegistryHelper().destroy();
    }

    /**
     * 注册任务源信息
     * @author  xiaoqianbin
     * @date    2020/8/16
     **/
    protected void registerTaskMeta() {
        getRegistryHelper().registerTaskMeta(getTaskName(), new TaskMeta(this), true);
    }

    /**
     * 获取当前主机名
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    protected String getHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "UnknownHost";
        }
    }

    /**
     * 获取主节点名
     * @author xiaoqianbin
     * @date 2020/7/14
     **/
    protected String getLeaderName() {
        return getHostName();
    }
}
