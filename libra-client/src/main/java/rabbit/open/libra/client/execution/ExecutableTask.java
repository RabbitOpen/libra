package rabbit.open.libra.client.execution;

/**
 * 可执行的任务
 * @author xiaoqianbin
 * @date 2020/7/13
 **/
public class ExecutableTask implements Runnable {

    private Runnable task;

    // 任务上级path
    private String path;

    // 任务分片节点名
    private String node;

    public ExecutableTask(Runnable task, String path, String node) {
        this.task = task;
        this.path = path;
        this.node = node;
    }

    @Override
    public void run() {
        getTask().run();
    }

    public Runnable getTask() {
        return task;
    }

    public String getPath() {
        return path;
    }

    public String getNode() {
        return node;
    }

}
