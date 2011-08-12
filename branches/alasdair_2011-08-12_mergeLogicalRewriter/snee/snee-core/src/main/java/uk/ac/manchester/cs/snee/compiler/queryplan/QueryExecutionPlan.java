package uk.ac.manchester.cs.snee.compiler.queryplan;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

/**
 * Abstract Query Plan class.
 */
public abstract class QueryExecutionPlan {

	/**
	 * Logger for this class.
	 */
	private Logger logger = 
		Logger.getLogger(QueryExecutionPlan.class.getName());
	
	/**
	 * Identifier of this query plan.
	 */
	String id;
	
	/**
	 * Stores ResultSet style metadata about the query plan
	 */
	protected QueryPlanMetadata metadata;
	
	/**
	 * Counter used to assign unique id to different candidates.
	 */
	protected static int candidateCount = 0;
	
	/**
	 * Underlying DLAF.
	 */
	DLAF dlaf;
	
	/**
	 * Constructor for QueryExecutionPlan.
	 * @param dlaf
	 * @param queryName
	 */
	protected QueryExecutionPlan(DLAF dlaf, String queryName)
	throws SchemaMetadataException, TypeMappingException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER QueryExecutionPlan()"); 
		this.id = generateName(queryName);
		this.dlaf = dlaf;

		if (logger.isDebugEnabled())
			logger.debug("RETURN QueryExecutionPlan()"); 
	}

	public QueryPlanMetadata getMetaData() {
		return metadata;
	}
	
	/**
	 * Gets the underlying DLAF.
	 * @return
	 */
	public DLAF getDLAF(){
		if (logger.isDebugEnabled())
			logger.debug("ENTER getDLAF()"); 
		if (logger.isDebugEnabled())
			logger.debug("RETURN getDLAF()"); 
		return this.dlaf;
	}

	/**
	 * Gets the underlying LAF.
	 * @return
	 */
	public LAF getLAF() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getLAF()"); 
		if (logger.isDebugEnabled())
			logger.debug("RETURN getLAF()"); 
		return this.dlaf.getLAF();
	}

	/**
	 * Gets the query plan identifier.
	 * @return
	 */
	public String getID() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getID()"); 
		if (logger.isDebugEnabled())
			logger.debug("RETURN getID()"); 
		return this.id;
	}
	
	/**
	 * Resets the candidate counter; use prior to compiling the next query.
	 */
	public static void resetCandidateCounter() {
		candidateCount = 0;
	}

	/**
	 * Generates a systematic name for this query plan structure, 
	 * of the form
	 * {query-name}-{structure-type}-{counter}.
	 * @param queryName	The name of the query
	 * @return the generated name for the query plan structure
	 */
	private String generateName(String queryName) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER generateName()"); 
		candidateCount++;
		if (logger.isDebugEnabled())
			logger.debug("RETURN generateName()"); 
		return queryName + "-QEP-" + candidateCount;
	}

	//delegate
	public String getQueryName() {
		return this.getLAF().getQueryName();
	}
}
