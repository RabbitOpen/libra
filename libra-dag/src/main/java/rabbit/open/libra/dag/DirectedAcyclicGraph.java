package rabbit.open.libra.dag;

import rabbit.open.libra.dag.exception.CyclicDagException;
import rabbit.open.libra.dag.exception.NoPathException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * dag图对象
 * @author xiaoqianbin
 * @date 2020/8/7
 **/
@SuppressWarnings({"serial", "unchecked"})
public abstract class DirectedAcyclicGraph<T extends DagNode> implements Serializable {

	/**
	 * 	图的头
	 */
    private T head;

    /**
	 * 	图的尾部
	 */
    private T tail;

    /**
     * 所有节点
     **/
    private Set<T> nodes = new HashSet<>();

	/**
	 * 正在调度的节点
	 **/
    private Set<T> runningNodes;

	/**
	 * 正在调度的节点队列锁
	 **/
    private ReentrantLock runningNodesQueueLock = new ReentrantLock();

    /**
	 * 	图包含的路径
	 */
    private List<List<T>> paths = new ArrayList<>();

    public DirectedAcyclicGraph(T head, T tail) {
        this(head, tail, 64);
    }

    /**
     *
     * @param	head
	 * @param	tail
	 * @param	maxNodeSize 	图中最大节点个数
     * @author  xiaoqianbin
     * @date    2020/8/10
     **/
	public DirectedAcyclicGraph(T head, T tail, int maxNodeSize) {
		this.head = head;
		this.tail = tail;
		if (head.getNextNodes().isEmpty()) {
			throw new NoPathException();
		}
		runningNodes = new HashSet<>(maxNodeSize);
		doCycleChecking();
		loadNodes(this.head);
	}

	/**
	 * 加载图中所有节点
	 * @param	h
	 * @author  xiaoqianbin
	 * @date    2020/8/10
	 **/
	protected void loadNodes(T h) {
		nodes.add(h);
		h.setGraph(this);
		for (DagNode nextNode : h.getNextNodes()) {
			loadNodes((T)nextNode);
		}
	}

    /**
     * 执行调度
     * @author  xiaoqianbin
     * @date    2020/8/10
     **/
    public void startSchedule() {
		if (runningNodes.isEmpty()) {
			addRunningNode(head);
			head.setScheduleStatus(ScheduleStatus.RUNNING);
			saveGraph();
			scheduleDagNode((T) head);
		}
    }

    /**
     * dag node 被执行了
     * @param	node
     * @author  xiaoqianbin
     * @date    2020/8/10
     **/
    public void onDagNodeExecuted(DagNode node) {
		removeRunningNode(node);
		node.setScheduleStatus(ScheduleStatus.FINISHED);
    	if (node != tail) {
			for (DagNode nextNode : node.getNextNodes()) {
				addRunningNode((T) nextNode);
				nextNode.setScheduleStatus(ScheduleStatus.RUNNING);
			}
			saveGraph();
			for (DagNode nextNode : node.getNextNodes()) {
			 	scheduleDagNode((T) nextNode);
			}
		} else {
			saveGraph();
    		onScheduleFinished();
		}
	}

	/**
	 * 添加节点到运行队列
	 * @param	node
	 * @author  xiaoqianbin
	 * @date    2020/8/10
	 **/
	private void removeRunningNode(DagNode node) {
    	try {
    		runningNodesQueueLock.lock();
			runningNodes.remove(node);
		} finally {
    		runningNodesQueueLock.unlock();
		}
	}

	/**
	 * 从运行队列中移除运行节点
	 * @param	nextNode
	 * @author  xiaoqianbin
	 * @date    2020/8/10
	 **/
	private void addRunningNode(T nextNode) {
		try {
			runningNodesQueueLock.lock();
			runningNodes.add(nextNode);
		} finally {
			runningNodesQueueLock.unlock();
		}
	}

	/**
	 * 当前dag调度完成
	 * @author  xiaoqianbin
	 * @date    2020/8/10
	 **/
	protected abstract void onScheduleFinished();

	/**
	 * 调度节点
	 * @param	node
	 * @author  xiaoqianbin
	 * @date    2020/8/10
	 **/
	protected void scheduleDagNode(T node) {
		if (node.getPreNodes().isEmpty()) {
			node.doSchedule();
		} else {
			for (DagNode preNode : node.getPreNodes()) {
				if (!preNode.isScheduled()) {
					return;
				}
			}
			node.doSchedule();
		}
	}

    /**
     * 持久化dag
     * @author  xiaoqianbin
     * @date    2020/8/10
     **/
    protected abstract void saveGraph();

    /**
     * <b>@description 环检测 </b>
     */
	public void doCycleChecking() {
		paths.clear();
		ArrayList<T> path = new ArrayList<>();
        path.add(this.head);
		doCycleChecking(this.head, path);
	}

	protected void doCycleChecking(T from, List<T> path) {
		List<T> pathList = new ArrayList<>(path);
		for (DagNode nextNode : from.getNextNodes()) {
			T node = (T) nextNode;
			if (node == tail) {
				// 搜索完一条路径
				List<T> p = new ArrayList<>();
				p.addAll(pathList);
				p.add(node);
				paths.add(p);
				continue;
			}
			if (!pathList.contains(node)) {
				pathList.add(node);
				// 搜索子路径
				doCycleChecking(node, pathList);
				// 搜索下一个分支
				pathList = new ArrayList<>(path);
			} else {
				throw new CyclicDagException("cycle is existed in this graph");
			}
		}
	}
	
	public List<List<T>> getPaths() {
		return paths;
	}

	public Set<T> getNodes() {
		return nodes;
	}

	public Set<T> getRunningNodes() {
		return runningNodes;
	}

	public T getHead() {
		return head;
	}

}
