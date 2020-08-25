package rabbit.open.dag;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rabbit.open.libra.dag.DagNode;
import rabbit.open.libra.dag.DirectedAcyclicGraph;
import rabbit.open.libra.dag.ScheduleStatus;
import rabbit.open.libra.dag.exception.CyclicDagException;
import rabbit.open.libra.dag.exception.DagException;
import rabbit.open.libra.dag.exception.NoPathException;

import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author xiaoqianbin
 * @date 2020/8/8
 **/
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class DagTest {

    private Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * <b>@description 简单路径扫描测试 </b>
     */
    @Test
    public void simplePathScanTest1() {
        MyScheduleDagNode start = new MyScheduleDagNode("start");
        start.addNextNode(new MyScheduleDagNode("branch1"));
        start.addNextNode(new MyScheduleDagNode("branch2"));
        start.addNextNode(new MyScheduleDagNode("branch3"));
        MyScheduleDagNode end = new MyScheduleDagNode("end");
        MyScheduleDagNode task1 = new MyScheduleDagNode("task1");
        task1.addNextNode(end);
        start.getNextNodes().forEach(n -> n.addNextNode(task1));
        DirectedAcyclicGraph<MyScheduleDagNode> graph = new MyDag(start, end);
        List<List<MyScheduleDagNode>> paths = graph.getPaths();
        TestCase.assertEquals(3, paths.size());
        for (int i = 0; i < paths.size(); i++) {
            TestCase.assertEquals(String.format("[start, branch%d, task1, end]", i + 1), paths.get(i).toString());
            logger.info("{}", paths.get(i));
        }
    }

    @Test
    public void pathCheckingTest() {
        MyScheduleDagNode start = new MyScheduleDagNode("start");
        start.addNextNode(new MyScheduleDagNode("branch01"));
        start.addNextNode(new MyScheduleDagNode("branch02"));
        MyScheduleDagNode end = new MyScheduleDagNode("end");
        MyScheduleDagNode task1 = new MyScheduleDagNode("task1");
        task1.addNextNode(end);
        start.getNextNodes().forEach(n -> n.addNextNode(task1));
        MyScheduleDagNode branch03 = new MyScheduleDagNode("branch03");
        start.addNextNode(branch03);
        branch03.addNextNode(new MyScheduleDagNode("branch031"));
        branch03.addNextNode(new MyScheduleDagNode("branch032"));
        try {
            new MyDag(start, end);
            throw new RuntimeException("");
        } catch (DagException e) {
            logger.info(e.getMessage());
        }
    }

    /**
     * 重新调度测试
     * @author xiaoqianbin
     * @date 2020/8/25
     **/
    @Test
    public void reStartScheduleTest() throws InterruptedException {
        MyScheduleDagNode start = new MyScheduleDagNode("start");
        AtomicLong counter = new AtomicLong(0);
        MyScheduleDagNode branch01 = new MyScheduleDagNode("branch01") {
            @Override
            public void doSchedule() {
                counter.getAndAdd(3L);
                super.doSchedule();
            }
        };
        start.addNextNode(branch01);
        MyScheduleDagNode branch02 = new MyScheduleDagNode("branch02") {
            @Override
            public void doSchedule() {
                counter.getAndAdd(3L);
                super.doSchedule();
            }
        };
        start.addNextNode(branch02);
        MyScheduleDagNode end = new MyScheduleDagNode("end");
        MyScheduleDagNode task1 = new MyScheduleDagNode("task1") {
            @Override
            public void doSchedule() {
                counter.getAndAdd(1L);
                super.doSchedule();
            }
        };
        task1.addNextNode(end);
        start.getNextNodes().forEach(n -> n.addNextNode(task1));
        Semaphore s = new Semaphore(0);
        MyDag myDag = new MyDag(start, end) {
            @Override
            protected void onScheduleFinished() {
                s.release();
            }
        };
        myDag.getRunningNodes().add(branch02);
        myDag.getRunningNodes().add(branch01);
        myDag.startSchedule();
        s.acquire();
        TestCase.assertEquals(1 + 3 + 3, counter.get());
    }

    /**
     * <b>@description 简单路径扫描测试 </b>
     */
    @Test
    public void simplePathScanTest2() throws InterruptedException {
        MyScheduleDagNode start = new MyScheduleDagNode("start");
        MyScheduleDagNode b1 = new MyScheduleDagNode("branch1");
        start.addNextNode(b1);
        MyScheduleDagNode b2 = new MyScheduleDagNode("branch2");
        start.addNextNode(b2);
        MyScheduleDagNode b3 = new MyScheduleDagNode("branch3");
        b2.addNextNode(b3);
        MyScheduleDagNode b4 = new MyScheduleDagNode("branch4");
        b2.addNextNode(b4);
        b1.addNextNode(b4);
        MyScheduleDagNode b5 = new MyScheduleDagNode("branch5");
        b1.addNextNode(b5);
        MyScheduleDagNode end = new MyScheduleDagNode("end");
        MyScheduleDagNode task1 = new MyScheduleDagNode("task1");
        b3.addNextNode(task1);
        b4.addNextNode(task1);
        b5.addNextNode(task1);
        task1.addNextNode(end);
        Semaphore s = new Semaphore(0);
        DirectedAcyclicGraph<MyScheduleDagNode> graph = new MyDag(start, end) {
            @Override
            protected void onScheduleFinished() {
                s.release();
            }
        };
        List<List<MyScheduleDagNode>> paths = graph.getPaths();
        TestCase.assertEquals(4, paths.size());
        TestCase.assertEquals(8, graph.getNodes().size());
        for (int i = 0; i < paths.size(); i++) {
            System.out.println(paths.get(i));
        }
        counter = new AtomicLong(0);
        graph.startSchedule();
        s.acquire();
        TestCase.assertEquals(16, counter.get());

        for (MyScheduleDagNode node : graph.getNodes()) {
            TestCase.assertEquals(node.getScheduleStatus(), ScheduleStatus.FINISHED);
        }

        TestCase.assertTrue(graph.getRunningNodes().isEmpty());
        TestCase.assertEquals(8, graph.getNodes().size());
    }

    /**
     * <b>@description 简单路径扫描测试 </b>
     */
    @Test
    public void simplePathScanTest3() {
        MyScheduleDagNode end = new MyScheduleDagNode("end");
        MyScheduleDagNode start = new MyScheduleDagNode("start");
        MyScheduleDagNode t1 = new MyScheduleDagNode("task1");
        start.addNextNode(t1);
        MyScheduleDagNode t2 = new MyScheduleDagNode("task2");
        t1.addNextNode(t2);
        MyScheduleDagNode t3 = new MyScheduleDagNode("task3");
        t2.addNextNode(t3);
        t3.addNextNode(end);
        MyScheduleDagNode t4 = new MyScheduleDagNode("task4");
        t1.addNextNode(t4);
        t4.addNextNode(end);
        DirectedAcyclicGraph<MyScheduleDagNode> graph = new MyDag(start, end);
        List<List<MyScheduleDagNode>> paths = graph.getPaths();
        TestCase.assertEquals(2, paths.size());
        for (int i = 0; i < paths.size(); i++) {
            logger.info("{}", paths.get(i));
        }
        TestCase.assertEquals("[start, task1, task2, task3, end]", paths.get(0).toString());
        TestCase.assertEquals("[start, task1, task4, end]", paths.get(1).toString());
    }

    /**
     * <b>@description 环路异常 </b>
     */
    @Test
    public void cycleExceptionTest() {
        MyScheduleDagNode end = new MyScheduleDagNode("end");
        MyScheduleDagNode start = new MyScheduleDagNode("start");
        try {
            new MyDag(start, end);
            throw new RuntimeException();
        } catch (NoPathException e) {
            // TO DO: handle exception
        }
        MyScheduleDagNode t1 = new MyScheduleDagNode("task1");
        start.addNextNode(t1);
        MyScheduleDagNode t2 = new MyScheduleDagNode("task2");
        t1.addNextNode(t2);
        t2.addNextNode(end);
        DirectedAcyclicGraph<MyScheduleDagNode> graph = new MyDag(start, end);
        TestCase.assertEquals(1, graph.getPaths().size());
        TestCase.assertEquals("[start, task1, task2, end]", graph.getPaths().get(0).toString());
        t2.addNextNode(t1);
        try {
            graph.doCycleChecking();
            throw new RuntimeException();
        } catch (CyclicDagException e) {
            // TO DO: handle exception
        }
    }

    private AtomicLong counter = new AtomicLong(0);

    public class MyScheduleDagNode extends DagNode {

        String nodeName;

        public MyScheduleDagNode(String nodeName) {
            this.nodeName = nodeName;
        }

        @Override
        public String toString() {
            return nodeName;
        }

        @Override
        public void doSchedule() {
            counter.getAndAdd(2L);
            graph.onDagNodeExecuted(this);
        }

        @Override
        protected boolean isScheduled() {
            return ScheduleStatus.FINISHED == scheduleStatus;
        }
    }

    public class MyDag extends DirectedAcyclicGraph<MyScheduleDagNode> {

        public MyDag(MyScheduleDagNode head, MyScheduleDagNode tail) {
            super(head, tail);
        }

        @Override
        protected void onScheduleFinished() {

        }

        @Override
        protected void saveGraph() {

        }
    }
}
