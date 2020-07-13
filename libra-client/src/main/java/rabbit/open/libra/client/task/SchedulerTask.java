package rabbit.open.libra.client.task;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import rabbit.open.libra.client.AbstractLibraTask;
import rabbit.open.libra.client.ExecuteType;
import rabbit.open.libra.client.RegistryHelper;

import java.util.concurrent.Semaphore;

/**
 * 调度任务，分布式任务的调度核心，负责调度所有其他任务
 * @author xiaoqianbin
 * @date 2020/7/11
 **/
public class SchedulerTask extends AbstractLibraTask {

    private Logger logger = LoggerFactory.getLogger(getClass());

    public final static String GROUP_NAME = "SYSTEM";

    private Thread schedulerThread;

    private boolean closed = false;

    @Autowired
    private RegistryHelper helper;

    /**
     * 监控线程阻塞信号
     * @author xiaoqianbin
     * @date 2020/7/13
     **/
    private Semaphore blockingSemaphore = new Semaphore(0);

    @Override
    public RegistryHelper getRegistryHelper() {
        return helper;
    }

    /**
     * 执行任务
     * @param	index
	 * @param	splits
	 * @param	executeTime
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    @Override
    public void execute(int index, int splits, String executeTime) {
        ZkClient zkClient = getRegistryHelper().getClient();
        String sysNode = getRegistryHelper().getRootPath() + "/tasks/execution/system";
        String sysPath = sysNode + "/" + getTaskName();
        zkClient.subscribeChildChanges(sysNode, (path, list) -> {
            logger.info("path 【{}】 children changed, {}", path, list);
            if (!list.contains(getTaskName()) && try2AcquireControl(sysPath, CreateMode.EPHEMERAL)) {
                logger.info("running SchedulerTask in [active] mode");
                blockingSemaphore.release();
            }
        });
        if (try2AcquireControl(sysPath, CreateMode.EPHEMERAL)) {
            logger.info("running SchedulerTask in [active] mode");
            blockingSemaphore.release();
        } else {
            logger.info("running SchedulerTask in [standby] mode");
        }
        schedulerThread = new Thread(() -> {
            while (true) {
                try {
                    blockingSemaphore.acquire();
                    if (closed) {
                        break;
                    }
                    doSchedule();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
            logger.info("scheduler thread is exited....");
        });
        schedulerThread.setDaemon(false);
        schedulerThread.start();
    }

    /**
     * 执行调度
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    protected void doSchedule() {
        loadTaskMetas();
        monitorTaskMetas();
    }

    /**
     * 加载任务元信息
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    protected void loadTaskMetas() {

    }

    /**
     * 监听任务元信息
     * @author  xiaoqianbin
     * @date    2020/7/13
     **/
    protected void monitorTaskMetas() {

    }

    @Override
    protected ExecuteType getExecuteType() {
        return ExecuteType.ACTIVE;
    }

    @Override
    protected String getTaskGroup() {
        return GROUP_NAME;
    }

    @Override
    protected boolean isSystemTask() {
        return true;
    }

    @Override
    protected void close() {
        logger.info("scheduler is closing......");
        closed = true;
        blockingSemaphore.release();
    }
}
