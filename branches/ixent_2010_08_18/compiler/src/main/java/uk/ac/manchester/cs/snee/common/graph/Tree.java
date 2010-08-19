package uk.ac.manchester.cs.snee.common.graph;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class Tree extends Graph {

	private Node root;
	
//	private HashSet<Node> leaves;

	public Tree(Node root) {
		this.root = root;
		this.updateNodesAndEdgesColls(this.root);
	}

	/**
	 * Updates the nodes and edges collections according to the tree 
	 * passed to it.
	 * @param node The current operator being processed
	 */
	public void updateNodesAndEdgesColls(Node node) {
		this.nodes.put(node.getID(), node);

		/* Post-order traversal of operator tree */
		if (!node.isLeaf()) {
			for (int childIndex = 0; childIndex < node.getInDegree(); 
			childIndex++) {
				LogicalOperator c = (LogicalOperator) node.getInput(childIndex);

				this.updateNodesAndEdgesColls(c);
				EdgeImplementation e = new EdgeImplementation(this
						.generateEdgeID(c.getID(), node.getID()), c.getID(), node
						.getID());
				this.edges.put(this.generateEdgeID(c.getID(), node.getID()), e);
			}
		}
	}
	
	public Node getRoot() {
		return this.root;
	}
	
	/**
	 * Helper method to recursively generate the operator iterator.
	 * @param node the operator being visited
	 * @param nodeList the operator list being created
	 * @param traversalOrder the traversal order desired 
	 */
	public <N extends Node> void doNodeIterator(N node,
			ArrayList<N> nodeList, 
			TraversalOrder traversalOrder) {

		if (traversalOrder == TraversalOrder.PRE_ORDER) {
			nodeList.add(node);
		}

		for (int n = 0; n < node.getInDegree(); n++) {
			this.doNodeIterator((N)node.getInput(n), nodeList, traversalOrder);
		}

		if (traversalOrder == TraversalOrder.POST_ORDER) {
			nodeList.add(node);
		}
	}	

	/**
	 * Iterator to traverse the operator tree.
	 * The structure of the operator tree may not be modified during 
	 * iteration
	 * @param traversalOrder the order to traverse the operator tree
	 * @return an iterator for the operator tree
	 */
	public <N extends Node> Iterator<N> nodeIterator(
			TraversalOrder traversalOrder) {

		ArrayList<N> nodeList = 
			new ArrayList<N>();
		this.doNodeIterator((N)this.getRoot(), nodeList, 
				traversalOrder);

		return nodeList.iterator();
	}
}
