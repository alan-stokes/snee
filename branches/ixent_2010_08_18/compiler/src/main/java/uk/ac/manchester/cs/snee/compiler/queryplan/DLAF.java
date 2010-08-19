package uk.ac.manchester.cs.snee.compiler.queryplan;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.graph.Tree;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceMetadata;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceType;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetDeliverOperator;

/**
 * The logical algebraic form of the query plan operator tree partitioned
 * by source (i.e., sensor network, evaluator).  For now, query plans are
 * either executed wholly within the sensor network, or wholly within the
 * evaluator.
 *
 */
public class DLAF extends SNEEAlgebraicForm {

    /**
     * Logger for this class.
     */
    private static  Logger logger = Logger.getLogger(DLAF.class.getName());
	
	/**
	 * The logical-algebraic form of the query plan operator tree from which
	 * DLAF is derived.
	 */	
	private LAF laf;

	/**
	 * The type of source this query operator tree is for.  In the future,
	 * it will be possible to allocate different portions of the LAF to different
	 * sourceTypes.
	 */
	private SourceType sourceType;

	/**
	 * The source that this query operator tree is for.  In the future,
	 * it will be possible to allocate different portions of the LAF to different
	 * sources.
	 */
	private SourceMetadata source;
	
    /**
     * Counter to assign unique id to different candidates.
     */
    protected static int candidateCount = 0;
    
	/**
	 * Constructor for DLAF
	 * @param laf The logical-algebraic form of the query plan operator tree 
	 * from which DLAF is derived.
	 * @param queryName The name of the query
	 */
	public DLAF(final LAF laf, final String queryName) {
		super(queryName);
		this.laf = laf;
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
    protected String generateName(final String queryName) {
    	candidateCount++;
    	return queryName + "-DLAF-" + candidateCount;
    }
    
	public String getProvenanceString() {
		return this.getName()+"-"+this.laf.getProvenanceString();
	}

	public void setSourceType(SourceType sourceType) {
		this.sourceType = sourceType;
	}

	public void setSource(SourceMetadata source) {
		this.source = source;
	}
	
	public SourceType getSourceType() {
		return this.sourceType;
	}
	
	public SourceMetadata getSource() {
		return this.source;
	}

	public LAF getLAF() {
		return this.laf;
	}

	public LogicalOperator getRootOperator() {
		return this.getLAF().getRootOperator();
	}

	public Tree getOperatorTree() {
		return this.getLAF().getOperatorTree();
	}
	
}
