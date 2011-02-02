package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.graph.Tree;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

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
    private Logger logger = Logger.getLogger(DLAF.class.getName());
	
	/**
	 * The logical-algebraic form of the query plan operator tree 
	 * from which DLAF is derived.
	 */	
	private LAF laf;

	/**
	 * The sources that are used in this query operator tree.
	 */
	private List<SourceMetadataAbstract> sources = 
		new ArrayList<SourceMetadataAbstract>();
	
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
		if (logger.isDebugEnabled())
			logger.debug("ENTER DLAF()");
		this.laf = laf;
		if (logger.isDebugEnabled())
			logger.debug("RETURN DLAF()");
	}
	
    /**
     * Resets the counter; use prior to compiling the next query.
     */
    public static void resetCandidateCounter() {
    	candidateCount = 0;
    }
	
	 /** {@inheritDoc} */
    protected String generateID(final String queryName) {
//		if (logger.isTraceEnabled())
//			logger.trace("ENTER generateName()");    	
    	candidateCount++;
//		if (logger.isTraceEnabled())
//			logger.trace("ENTER generateName()");       	
    	return queryName + "-DLAF-" + candidateCount;
    }
    
//
//	public void setSourceType(SourceType sourceType) {
//		this.sourceType = sourceType;
//	}

    /** {@inheritDoc} */
	public String getDescendantsString() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getProvenanceString()");   
		if (logger.isDebugEnabled())
			logger.debug("RETURN getProvenanceString()");  
		return this.getID()+"-"+this.laf.getDescendantsString();
	}

	public void setSources(List<SourceMetadataAbstract> sources) {
		this.sources.addAll(sources);
	}

	public List<SourceMetadataAbstract> getSources() {
		return sources;
	}

	/**
	 * Get the LAF that this DLAF is descended from.
	 * @return
	 */
	public LAF getLAF() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getLAF()");
		if (logger.isDebugEnabled())
			logger.debug("RETURN getLAF()");		
		return this.laf;
	}

	/**
	 * Delegate method; gets root operator from LAF.
	 * @return
	 */
	public LogicalOperator getRootOperator() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getRootOperator()");
		if (logger.isDebugEnabled())
			logger.debug("RETURN getRootOperator()");		
		return this.getLAF().getRootOperator();
	}

	/**
	 * Delegate method; gets operator tree from LAF.
	 * @return
	 */
	public Tree getOperatorTree() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getOperatorTree()");
		if (logger.isDebugEnabled())
			logger.debug("RETURN getOperatorTree()");		
		return this.getLAF().getOperatorTree();
	}	
}
