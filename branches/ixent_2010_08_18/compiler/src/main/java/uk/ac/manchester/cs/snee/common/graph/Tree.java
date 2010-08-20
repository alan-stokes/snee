package uk.ac.manchester.cs.snee.common.graph;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.compiler.sn.router.Router;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class Tree extends Graph {

	/**
	 * Logger for this class.
	 */
	private Logger logger = 
		Logger.getLogger(Router.class.getName());
	
	/**
	 * The root of the tree.
	 */
	private Node root;
	
//	private HashSet<Node> leaves;

	/**
	 * Tree constructor
	 */
	public Tree(Node root) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER Tree()");
		this.root = root;
		this.updateNodesAndEdgesColls(this.root);
		if (logger.isDebugEnabled())
			logger.debug("RETURN Tree()");
	}

	/**
	 * Updates the nodes and edges collections according to the tree 
	 * passed to it.  Call this if you've updated the tree by adding nodes
	 * directly to other nodes.
	 * @param node The current operator being processed
	 */
	public void updateNodesAndEdgesColls(Node node) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER updateNodesAndEdgesColls()");
		this.nodes.put(node.getID(), node);

		/* Post-order traversal of operator tree */
		if (!node.isLeaf()) {
			for (int childIndex = 0; childIndex < node.getInDegree(); 
			childIndex++) {
				Node c = (Node) node.getInput(childIndex);

				this.updateNodesAndEdgesColls(c);
				EdgeImplementation e = new EdgeImplementation(this
						.generateEdgeID(c.getID(), node.getID()), c.getID(), node
						.getID());
				this.edges.put(this.generateEdgeID(c.getID(), node.getID()), e);
			}
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN updateNodesAndEdgesColls()");
	}

	/**
	 * Get the root of the tree.
	 * @return
	 */
	public Node getRoot() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getRoot()");
		if (logger.isDebugEnabled())
			logger.debug("RETURN getRoot()");		
		return this.root;
	}
	
	/**
	 * Helper method to recursively generate the operator iterator.
	 * @param node the operator being visited
	 * @param nodeList the operator list being created
	 * @param traversalOrder the traversal order desired 
	 */
	private <N extends Node> void doNodeIterator(N node,
			ArrayList<N> nodeList, 
			TraversalOrder traversalOrder) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER doNodeIterator()");
		if (traversalOrder == TraversalOrder.PRE_ORDER) {
			nodeList.add(node);
		}
		for (int n = 0; n < node.getInDegree(); n++) {
			this.doNodeIterator((N)node.getInput(n), nodeList, traversalOrder);
		}
		if (traversalOrder == TraversalOrder.POST_ORDER) {
			nodeList.add(node);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN doNodeIterator()");
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
		if (logger.isDebugEnabled())
			logger.debug("ENTER nodeIterator()");
		ArrayList<N> nodeList = 
			new ArrayList<N>();
		this.doNodeIterator((N)this.getRoot(), nodeList, 
				traversalOrder);
		if (logger.isDebugEnabled())
			logger.debug("RETURN nodeIterator()");
		return nodeList.iterator();
	}

}
