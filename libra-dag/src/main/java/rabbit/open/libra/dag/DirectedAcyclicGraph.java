package rabbit.open.libra.dag;

import java.util.ArrayList;
import java.util.List;

import rabbit.open.libra.dag.exception.CyclicDagException;
import rabbit.open.libra.dag.exception.NoPathException;

/**
 * dag图对象
 * @author xiaoqianbin
 * @date 2020/8/7
 **/
public class DirectedAcyclicGraph<T extends DagNode> {

	/**
	 * 	图的头
	 */
    private T head;

    /**
	 * 	图的尾部
	 */
    private T tail;
    
    /**
	 * 	图包含的路径
	 */
    private List<List<T>> paths = new ArrayList<>();

    public DirectedAcyclicGraph(T head, T tail) {
        this.head = head;
        this.tail = tail;
        if (head.getNextNodes().isEmpty()) {
        	throw new NoPathException();
        }
        doCycleChecking();
    }

    /**
     * <b>@description 环检测 </b>
     */
	public void doCycleChecking() {
		paths.clear();
		ArrayList<T> path = new ArrayList<>();
        path.add(this.head);
		doCycleChecking(this.head, path);
	}

	@SuppressWarnings("unchecked")
	protected void doCycleChecking(T from, List<T> path) {
		int resetIndex = path.size();
        for (DagNode nextNode : from.getNextNodes()) {
        	T node = (T)nextNode;
            if (node == tail) {
            	// 搜索完一条路径
            	List<T> p = new ArrayList<>();
            	p.addAll(path);
            	p.add(node);
            	paths.add(p);
            	continue;
            }
            if (!path.contains(node)) {
            	path.add(node);
            	// 搜索子路径
            	doCycleChecking(node, path);
            	// 搜索下一个分支
            	path = path.subList(0, resetIndex);
            } else {
            	throw new CyclicDagException("cycle is existed in this graph");
            }
        }
	}
	
	public List<List<T>> getPaths() {
		return paths;
	}

}
