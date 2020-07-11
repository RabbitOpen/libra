package rabbit.open.libra.client;

import rabbit.open.libra.client.TaskPiece;

/**
 * task meta 信息
 * @author xiaoqianbin
 * @date 2020/7/11
 **/
public class TaskMeta {

    // 任务片
    private TaskPiece taskPiece;

    public TaskMeta(TaskPiece taskPiece) {
        this.taskPiece = taskPiece;
    }

    public TaskPiece getTaskPiece() {
        return taskPiece;
    }
}
