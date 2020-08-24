package rabbit.open.libra.test;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import rabbit.open.libra.client.Constant;
import rabbit.open.libra.client.RegistryHelper;
import rabbit.open.libra.client.dag.*;
import rabbit.open.libra.client.exception.RepeatedScheduleException;
import rabbit.open.libra.client.task.SchedulerTask;
import rabbit.open.libra.test.tasks.MySchedulerTask;
import rabbit.open.libra.test.tasks.SimpleTask;

import javax.annotation.Resource;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * @author xiaoqianbin
 * @date 2020/8/14
 **/
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "classpath*:/applicationContext.xml" })
public class DistributedTaskTest {

	private Logger logger = LoggerFactory.getLogger(getClass());

	@Resource
	MySchedulerTask mySchedulerTask;

	@Resource
	SchedulerTask schedulerTask;

	@Autowired
	SimpleTask task;

	/**
	 * <b>@description 简单测试 </b>
	 * @throws NoSuchFieldException
	 * @throws IllegalAccessException
	 * @throws InterruptedException
	 */
	@Test
	public void simpleTest() throws NoSuchFieldException, IllegalAccessException, InterruptedException {
		TestCase.assertEquals(schedulerTask, mySchedulerTask);
		MyDagTaskNode head = new MyDagTaskNode("head");
		DagTaskNode task = new MyDagTaskNode("task");
		DagTaskNode tail = new MyDagTaskNode("tail");
		head.addNextNode(task);
		task.addNextNode(tail);
		Semaphore s = new Semaphore(0);
		SchedulableDirectedAcyclicGraph dag = new SchedulableDirectedAcyclicGraph(head, tail);
		String dagId = "my-dag-id";
		dag.setDagName("测试dag");
		dag.setCronExpression("0 * 1 * * *");
		dag.setDagId(dagId);
		mySchedulerTask.getRegistryHelper().deleteRecursive(RegistryHelper.GRAPHS + Constant.SP + dag.getDagId());
		mySchedulerTask.createGraphNode(dag);
		SchedulableDirectedAcyclicGraph o = mySchedulerTask.getRegistryHelper()
				.readData(RegistryHelper.GRAPHS + Constant.SP + dag.getDagId());
		TestCase.assertEquals(o.getDagName(), dag.getDagName());
		Map<String, SchedulableDirectedAcyclicGraph> dagMetaMap = getObjectValue("dagMetaMap", mySchedulerTask,
				SchedulerTask.class);
		mySchedulerTask.setLoadMeta(() -> {
			s.release();
		});
		s.acquire();
		TestCase.assertEquals("head", dagMetaMap.get(dagId).getHead().getTaskName());
		s.drainPermits();
		head.setTaskName("newHead");
		mySchedulerTask.updateDagInfo(dag, () -> {
			s.release();
		});
		s.acquire();
		TestCase.assertEquals(dagMetaMap.get(dagId).getHead().getTaskName(), "newHead");
	}

	/**
	 * 反射取值
	 * @param field 字段名
	 * @param obj   对象
	 * @param clz   字段所属的类（obj的类或者obj的父类）
	 * @author xiaoqianbin
	 * @date 2020/8/20
	 **/
	@SuppressWarnings("unchecked")
	private <T> T getObjectValue(String field, Object obj, Class<?> clz)
			throws NoSuchFieldException, IllegalAccessException {
		Field f = clz.getDeclaredField(field);
		f.setAccessible(true);
		return (T) f.get(obj);
	}

	@Test
	public void scheduleTest() throws InterruptedException, NoSuchFieldException, IllegalAccessException {
		DagHeader header = new DagHeader();
		DagTail tail = new DagTail();
		Semaphore quit = new Semaphore(0);
		task.setTask(ctx -> {
			logger.info("schedule date: {}", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ctx.getScheduleDate()));
			quit.release();
		});
		mySchedulerTask.setScheduleFinished(() -> {
			quit.release();
		});
		DagTaskNode taskNode = new DagTaskNode(task.getTaskName(), 2, task.getSplitsCount(), task.getAppName());
		header.addNextNode(taskNode);
		taskNode.addNextNode(tail);
		RuntimeDagInstance graph = new RuntimeDagInstance(header, tail, 64);
		String dagId = "schedule-test-graph";
		graph.setDagName("测试dag");
		graph.setCronExpression("0 * 1 * * *");
		graph.setDagId(dagId);
		mySchedulerTask.getRegistryHelper().deleteRecursive(RegistryHelper.GRAPHS + Constant.SP + graph.getDagId());
		logger.info("**** delete dag {}", dagId);
		Map<String, SchedulableDirectedAcyclicGraph> dagMetaMap = getObjectValue("dagMetaMap", mySchedulerTask,
				SchedulerTask.class);
		dagMetaMap.remove(graph.getDagId());
		mySchedulerTask.createGraphNode(graph);
		logger.info("**** create dag {}", dagId);
		mySchedulerTask.scheduleGraph(graph);
		logger.info("**** schedule dag {}", dagId);
		try {
			mySchedulerTask.scheduleGraph(graph);
			throw new RuntimeException("不可能走到这里");
		} catch (RepeatedScheduleException e) {
			
		}
		quit.acquire(2);
	}


}
