package rabbit.open.libra.client.task;

/**
 * 可执行的任务
 * @author xiaoqianbin
 * @date 2020/7/13
 **/
public class ExecutableTask implements Runnable {

    private Runnable task;

    private String path;

    private String node;

    public ExecutableTask(Runnable task, String path, String node) {
        this.task = task;
        this.path = path;
        this.node = node;
    }

    @Override
    public void run() {
        task.run();
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
