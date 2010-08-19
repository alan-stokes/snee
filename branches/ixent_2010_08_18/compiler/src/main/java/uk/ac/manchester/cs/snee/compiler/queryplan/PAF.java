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
	Tree physicalOperatorTree;
	
    
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
		this.dlaf=dlaf;
		DeliverOperator logDelOp = 
			(DeliverOperator) dlaf.getRootOperator();
		SensornetDeliverOperator phyDelOp =
			new SensornetDeliverOperator(logDelOp);
		this.physicalOperatorTree = new Tree(phyDelOp);
	}

    /**
     * Resets the counter; use prior to compiling the next query.
     */
    public static void resetCandidateCounter() {
    	candidateCount = 0;
    }

	public DLAF getDLAF() {
		return dlaf;
	}

	/**
	 * Generates a systematic name for this query plan structure, of the form
	 * {query-name}-{structure-type}-{counter}.
	 * @param queryName	The name of the query
	 * @return the generated name for the query plan structure
	 */
	protected String generateName(String queryName) {
    	candidateCount++;
    	return queryName + "-PAF-" + candidateCount;	
    }

	public String getProvenanceString() {
		return this.getName()+"-"+this.dlaf.getProvenanceString();
	}

	public void replacePath(SensornetOperator op, SensornetOperator[] nodes) {
		this.physicalOperatorTree.replacePath(op, nodes);
	}

	public void insertNode(SensornetAggrInitOperator child,
			SensornetAggrEvalOperator parent,
			SensornetAggrMergeOperator newNode) {
		this.physicalOperatorTree.insertNode(child, parent, newNode);
	}
	
	public Iterator<SensornetOperator> operatorIterator(TraversalOrder order) {
		return this.physicalOperatorTree.nodeIterator(order);
	}

	public Tree getOperatorTree() {
		return this.physicalOperatorTree;
	}
}
