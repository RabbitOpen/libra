package rabbit.open.dag;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import rabbit.open.libra.dag.DirectedAcyclicGraph;
import rabbit.open.libra.dag.schedule.ScheduleContext;
import rabbit.open.libra.dag.schedule.ScheduleDagNode;

/**
 * @author xiaoqianbin
 * @date 2020/8/8
 **/
@RunWith(JUnit4.class)
public class DagTest {

    @Test
    public void scheduleTest() {

        MyScheduleDagNode start = new MyScheduleDagNode("start");
        start.addNextNode(new MyScheduleDagNode("branch1"));
        start.addNextNode(new MyScheduleDagNode("branch2"));
        MyScheduleDagNode end = new MyScheduleDagNode("end");
        start.getNextNodes().forEach(n -> n.addNextNode(end));
        DirectedAcyclicGraph<MyScheduleDagNode> graph = new DirectedAcyclicGraph<>(start);

    }

    public class MyScheduleDagNode extends ScheduleDagNode {

        String nodeName;

        protected boolean executed = false;

        public MyScheduleDagNode(String nodeName) {
            this.nodeName = nodeName;
        }

        @Override
        public void doScheduledJob(ScheduleContext context) {
            executed = true;
        }

        @Override
        protected boolean isScheduled(ScheduleContext context) {
            return executed;
        }
    }
}
