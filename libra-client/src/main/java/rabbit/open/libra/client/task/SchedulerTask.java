package rabbit.open.libra.client.task;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;
import rabbit.open.libra.client.*;
import rabbit.open.libra.client.dag.SchedulableDag;
import rabbit.open.libra.client.meta.TaskMeta;
import rabbit.open.libra.dag.schedule.ScheduleContext;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 调度任务
 * @author xiaoqianbin
 * @date 2020/8/16
 **/
@SuppressWarnings("serial")
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

    /**
     * listener map， key是path
     * @author  xiaoqianbin
     * @date    2020/8/18
     **/
    private Map<String, IZkChildListener> childChangedListenerMap = new ConcurrentHashMap<>();

    /**
     * 数据变更监听器
     * @author  xiaoqianbin
     * @date    2020/8/18
     **/
    private Map<String, IZkDataListener> dataChangedListenerMap = new ConcurrentHashMap<>();

    /**
     * dag meta信息 key 是dag id信息
     * @author  xiaoqianbin
     * @date    2020/8/18
     **/
    private Map<String, SchedulableDag> dagMetaMap = new ConcurrentHashMap<>();

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
        String schedulePath = RegistryHelper.META_CONTROLLER + Constant.SP + getTaskName();
        getRegistryHelper().subscribeChildChanges(RegistryHelper.META_CONTROLLER, (path, list) -> {
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
                        doSchedule();
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
     * 执行调度
     * @author  xiaoqianbin
     * @date    2020/8/18
     **/
    protected void doSchedule() {
        addDagReloadListener();
    }

    /**
     * 创建dag node
     * @param	dag
     * @author  xiaoqianbin
     * @date    2020/8/18
     **/
    public void createDagNode(SchedulableDag dag) {
        helper.create(RegistryHelper.GRAPHS + Constant.SP + dag.getDagMeta().getDagId(), dag, CreateMode.PERSISTENT);
    }

    /**
     * 监听dag变化(dag 变化时重新加载meta信息)
     * @author  xiaoqianbin
     * @date    2020/8/18
     **/
    protected void addDagReloadListener() {
        if (childChangedListenerMap.containsKey(RegistryHelper.GRAPHS)) {
            return;
        }
        IZkChildListener listener = (path, list) -> updateDagMeta(list);
        childChangedListenerMap.put(RegistryHelper.GRAPHS, listener);
        helper.subscribeChildChanges(RegistryHelper.GRAPHS, listener);
        loadDagMeta();
    }

    /**
     * 更新dag信息
     * @param	list
     * @author  xiaoqianbin
     * @date    2020/8/18
     **/
    private void updateDagMeta(List<String> list) {
        for (String dagId : list) {
            if (!dagMetaMap.containsKey(dagId)) {
                String relativePath = RegistryHelper.GRAPHS + Constant.SP + dagId;
                IZkDataListener listener = getDagDataChangeListener();
                dagMetaMap.put(dagId, helper.readData(relativePath));
                helper.subscribeDataChanges(relativePath, listener);
                dataChangedListenerMap.put(relativePath, listener);
            }
        }
        for (String id : dagMetaMap.keySet()) {
            if (!list.contains(id)) {
                dagMetaMap.remove(id);
                String relativePath = RegistryHelper.GRAPHS + Constant.SP + id;
                IZkDataListener dataListener = dataChangedListenerMap.remove(relativePath);
                helper.unsubscribeDataChanges(relativePath, dataListener);
            }
        }
    }

    /**
     * dag数据变更监听器
     * @author  xiaoqianbin
     * @date    2020/8/18
     **/
    private IZkDataListener getDagDataChangeListener() {
        return new IZkDataListener() {
            @Override
            public void handleDataChange(String path, Object data) throws Exception {
                String[] nodes = path.split("/");
                dagMetaMap.put(nodes[nodes.length -1], (SchedulableDag)data);
            }

            @Override
            public void handleDataDeleted(String s) throws Exception {
                // to do: i don't care
            }
        };
    }

    /**
     * 加载dag meta信息
     * @author  xiaoqianbin
     * @date    2020/8/18
     **/
    protected void loadDagMeta() {
        List<String> children = helper.getChildren(RegistryHelper.GRAPHS);
        for (String dagId : children) {
            String relativePath = RegistryHelper.GRAPHS + Constant.SP + dagId;
            dagMetaMap.put(dagId, helper.readData(relativePath));
            IZkDataListener listener = getDagDataChangeListener();
            helper.subscribeDataChanges(relativePath, listener);
            dataChangedListenerMap.put(relativePath, listener);
        }
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
        leader = false;
    }

    @Override
    protected void onZookeeperConnected() {
        if (leader) {
            return;
        }
        helper.registerExecutor();
        String schedulePath = RegistryHelper.META_CONTROLLER + Constant.SP + getTaskName();
        if (helper.exists(schedulePath)) {
            if (getLeaderName().equals(helper.readData(schedulePath))) {
                leader = true;
            }
        } else {
            try2AcquireControl(schedulePath, getLeaderName(), CreateMode.EPHEMERAL);
        }
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

    @Override
    public final String getTaskName() {
        return SchedulerTask.class.getSimpleName();
    }
}
