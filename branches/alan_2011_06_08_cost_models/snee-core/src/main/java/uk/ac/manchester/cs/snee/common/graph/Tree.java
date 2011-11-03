package uk.ac.manchester.cs.snee.common.graph;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class Tree extends Graph {

	/**
   * serialVersionUID
   */
  private static final long serialVersionUID = -3283091530591159897L;

  /**
	 * Logger for this class.
	 */
	private static final Logger logger = Logger.getLogger(Tree.class.getName());
	
	/**
	 * The root of the tree.
	 */
	private Node root;
//	private HashSet<Node> leaves;

	public void setRoot(Node root)
  {
    this.root = root;
  }

  /**
	 * Tree constructor
	 */
	public Tree(Node root, boolean update) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER Tree()");
		this.root = root;
		if(update)
		  this.updateNodesAndEdgesColls(this.root);
		if (logger.isDebugEnabled())
			logger.debug("RETURN Tree()");
	}

	public Tree()
  {
    super();
  }
	
  /**
	 * Updates the nodes and edges collections according to the tree 
	 * passed to it.  Call this if you've updated the tree by adding nodes
	 * directly to other nodes.
	 * @param node The current operator being processed
   * @param paf 
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
			logger.debug("ENTER/RETURN getRoot()");
		return this.root;
	}
	
	/**
	 * Helper method to recursively generate the operator iterator.
	 * @param node the operator being visited
	 * @param nodeList the operator list being created
	 * @param traversalOrder the traversal order desired 
	 */
	@SuppressWarnings("unchecked")
  private <N extends Node> void doNodeIterator(N node,
			ArrayList<N> nodeList, 
			TraversalOrder traversalOrder) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER doNodeIterator() with " + node.getID());
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
	@SuppressWarnings("unchecked")
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

  @SuppressWarnings("unchecked")
  public <N extends Node> Iterator<N> nodeIterator(
      Site rootSite,
      TraversalOrder traversalOrder)
  {
    if (logger.isDebugEnabled())
      logger.debug("ENTER nodeIterator()");
    ArrayList<N> nodeList = 
      new ArrayList<N>();
    this.doNodeIterator((N)rootSite, nodeList, 
        traversalOrder);
    if (logger.isDebugEnabled())
      logger.debug("RETURN nodeIterator()");
    return nodeList.iterator();
  }
  
  /**
   * returns all nodes in the tree which have no inputs (leaf nodes)
   * @return
   */
  public ArrayList<Node> getLeafNodes()
  {
    ArrayList<Node> leafNodes = new ArrayList<Node>();
    this.doLeafNodeIterator(this.getRoot(), leafNodes); 
    return leafNodes;
  }

  /**
   * helper method for get leaf nodes
   * @param root2
   * @param leafNodes
   */
  private void doLeafNodeIterator(Node node, ArrayList<Node> leafNodes)
  {
    if(node.isLeaf())
      leafNodes.add(node);
    else
    {
      for (int n = 0; n < node.getInDegree(); n++) 
      {
        this.doLeafNodeIterator(node.getInput(n), leafNodes);
      }
    }
  }

  public void removeSiteEdges(String nodeID)
  {
    ArrayList<Edge> toRemove = new ArrayList<Edge>();
    Iterator<Edge> edgeIter = this.edges.values().iterator();
    while (edgeIter.hasNext()) 
    {
      Edge e = edgeIter.next();
      if (e.getSourceID().equals(nodeID) || e.getDestID().equals(nodeID)) 
      {
        toRemove.add(e);
      }
    }
    edgeIter = toRemove.iterator();
    while (edgeIter.hasNext()) 
    {
      Edge e = edgeIter.next();
      String eid = generateEdgeID(e.getSourceID(),e.getDestID());
      this.edges.remove(eid);
    }
    
    
  }
}
