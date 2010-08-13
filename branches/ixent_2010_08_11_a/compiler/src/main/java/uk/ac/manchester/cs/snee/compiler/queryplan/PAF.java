package uk.ac.manchester.cs.snee.compiler.queryplan;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;

/**
 * The Sensor Network(?) Physical-algebraic form of the query plan operator tree.
 */
public class PAF extends DLAF {

	/**
	 * The logical-algebraic form of the query plan operator tree from which
	 * PAF is derived.
	 */
	private DLAF dlaf;
		
    /**
     * Logger for this class.
     */
    private static  Logger logger = Logger.getLogger(PAF.class.getName());
    
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
	 */
	public PAF(SensornetOperator deliverPhyOp, final DLAF dlaf, 
	final String queryName) {
		super(dlaf, generateName(queryName));
		this.dlaf = dlaf;
		this.rootOp = deliverPhyOp;
		this.updateNodesAndEdgesColls(this.rootOp);
	}
    
    /**
     * Constructor used by clone.
     * @param paf The PAF to be cloned
     * @param inName The name to be assigned to the data structure
     */
	public PAF(final PAF paf, final String inName) {
		super(paf, inName);
		this.dlaf = paf.dlaf; //This is ok because the dlaf is immutable now
		
	}

    /**
     * Resets the counter; use prior to compiling the next query.
     */
    public static void resetCandidateCounter() {
    	candidateCount = 0;
    }
	
	/**
	 * Generates a systematic name for this query plan structure, of the form
	 * {query-name}-{structure-type}-{counter}.
	 * @param queryName	The name of the query
	 * @return the generated name for the query plan structure
	 */
    private static String generateName(final String queryName) {
    	candidateCount++;
    	return queryName + "-PAF-" + candidateCount;
    }
	
//    protected void exportAsDOTFile(final String fname, 
//			TreeMap<String, StringBuffer> opLabelBuff,
//			TreeMap<String, StringBuffer> edgeLabelBuff,
//			StringBuffer fragmentsBuff) {
//    	
//	    final Iterator i = this.edges.keySet().iterator();
//	    while (i.hasNext()) {
//			final Edge e = this.edges.get((String) i.next());
//			final Operator sourceNode = (Operator) this.nodes
//				.get(e.getSourceID());
//			    
//			StringBuffer strBuff = new StringBuffer();
//			if (edgeLabelBuff.containsKey(e.getID())) {
//				strBuff = edgeLabelBuff.get(e.getID());
//			}
//			strBuff.append("Maximum cardinality: " 
//					+ ((Operator) sourceNode).
//					getCardinality(CardinalityType.PHYSICAL_MAX)	+ " \\n");
//			
//			edgeLabelBuff.put(e.getID(), strBuff);  
//
//	    }
//	    super.exportAsDOTFile(fname, "", opLabelBuff, edgeLabelBuff, 
//	    			fragmentsBuff);
//    }

//Should this be: replace LogicalOperator with SensorNetworkPhysicalOperator?    
    public void replace(final Node oldNode, final Node newNode) {
		final Node[] inputs = oldNode.getInputs();
		for (final Node n : inputs) {
		    n.replaceOutput(oldNode, newNode);
		    newNode.addInput(n);
		}
		final Node[] outputs = oldNode.getOutputs();
		for (final Node n : outputs) {
		    n.replaceInput(oldNode, newNode);
		    newNode.addOutput(n);
		}
		if (this.rootOp == oldNode) {
			this.rootOp = (LogicalOperator) newNode;
		}
		nodes.remove(oldNode.getID());
		nodes.put(newNode.getID(), newNode);
    }    
 
	public String getProvenanceString() {
		return this.dlaf.getProvenanceString() + "->" + this.name;
	}    
    
}
