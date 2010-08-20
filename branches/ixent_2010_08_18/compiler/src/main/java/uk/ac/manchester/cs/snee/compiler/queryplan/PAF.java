package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.graph.Graph;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.common.graph.Tree;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrEvalOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrInitOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAggrMergeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetDeliverOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;

/**
 * The Sensor Network(?) Physical-algebraic form of the query plan operator tree.
 */
public class PAF extends SNEEAlgebraicForm {

    /**
     * Logger for this class.
     */
    private static  Logger logger = Logger.getLogger(PAF.class.getName());
	
	/**
	 * The logical-algebraic form of the query plan operator tree from which
	 * PAF is derived.
	 */
	private DLAF dlaf;

	/**
	 * The tree of Physical operators
	 */
	private Tree physicalOperatorTree;
	
    
    /**
     * Counter to assign unique id to different candidates.
     */
    protected static int candidateCount = 0;
    
	/**
	 * Constructor for Physical-algebraic form.
	 * @param deliverPhyOp the physical operator which is the root of the tree
	 * @param dlaf The distributed logical-algebraic form of the query plan 
	 * operator tree from which PAF is derived.
	 * @param queryName The name of the query
	 * @throws SNEEException 
	 */
	public PAF(SensornetOperator deliverPhyOp, final DLAF dlaf, 
	final String queryName) throws SNEEException, SchemaMetadataException {
		super(queryName);
		if (logger.isDebugEnabled())
			logger.debug("ENTER PAF()"); 
		this.dlaf=dlaf;
		DeliverOperator logDelOp = 
			(DeliverOperator) dlaf.getRootOperator();
		SensornetDeliverOperator phyDelOp =
			new SensornetDeliverOperator(logDelOp);
		this.physicalOperatorTree = new Tree(phyDelOp);
		if (logger.isDebugEnabled())
			logger.debug("RETURN PAF()"); 	
	}

    /**
     * Resets the counter; use prior to compiling the next query.
     */
    public static void resetCandidateCounter() {
    	candidateCount = 0;
    }

    /**
     * Gets the DLAF that this PAF is associated with.
     * @return
     */
	public DLAF getDLAF() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getDLAF()"); 
		if (logger.isDebugEnabled())
			logger.debug("RETURN getDLAF()"); 
		return dlaf;
	}

	 /** {@inheritDoc} */
	protected String generateID(String queryName) {
//		if (logger.isDebugEnabled())
//			logger.debug("ENTER generateName()"); 
    	candidateCount++;
//		if (logger.isDebugEnabled())
//			logger.debug("RETURN generateName()"); 
    	return queryName + "-PAF-" + candidateCount;	
    }

	 /** {@inheritDoc} */
	public String getDescendantsString() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getDescendantsString()"); 
		if (logger.isDebugEnabled())
			logger.debug("RETURN getDescendantsString()"); 
		return this.getID()+"-"+this.dlaf.getDescendantsString();
	}

	/**
	 * Replace a single operator with a sequence of operators.
	 * @param op
	 * @param nodes
	 */
	public void replacePath(SensornetOperator op, SensornetOperator[] nodes) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER replacePath()"); 
		if (logger.isDebugEnabled())
			logger.debug("RETURN replacePath()"); 
		this.physicalOperatorTree.replacePath(op, nodes);
	}

	/**
	 * Inserts a new node between a given child and parent.
	 * @param child
	 * @param parent
	 * @param newNode
	 */
	public void insertNode(SensornetAggrInitOperator child,
			SensornetAggrEvalOperator parent,
			SensornetAggrMergeOperator newNode) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER insertNode()");
		this.physicalOperatorTree.insertNode(child, parent, newNode);
		if (logger.isDebugEnabled())
			logger.debug("RETURN insertNode()");
	}
	
	/**
	 * Generates an iterator to traverse operator tree.
	 * @param order
	 * @return
	 */
	public Iterator<SensornetOperator> operatorIterator(TraversalOrder order) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER operatorIterator()"); 
		if (logger.isDebugEnabled())
			logger.debug("RETURN operatorIterator()"); 
		return this.physicalOperatorTree.nodeIterator(order);
	}

	/**
	 * Returns the physical operator tree.
	 * @return
	 */
	public Tree getOperatorTree() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getOperatorTree()"); 
		if (logger.isDebugEnabled())
			logger.debug("RETURN getOperatorTree()"); 
		return this.physicalOperatorTree;
	}
}
