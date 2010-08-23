package uk.ac.manchester.cs.snee.compiler.queryplan;

import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

/**
 * Abstract Query Plan class.
 */
public abstract class QueryExecutionPlan {

	String name;
	
	/**
	 * Stores ResultSet style metadata about the query plan
	 */
	private QueryPlanMetadata metadata;
	
	/**
	 * Counter used to assign unique id to different candidates.
	 */
	protected static int candidateCount = 0;
	
	DLAF dlaf;
	
	protected QueryExecutionPlan(DLAF dlaf, String queryName) 
	throws SchemaMetadataException, TypeMappingException {
		this.name = generateName(queryName);
		this.dlaf = dlaf;
		LogicalOperator rootOperator = dlaf.getLAF().getRootOperator();
		metadata = new QueryPlanMetadata(rootOperator.getAttributes());
	}

	public QueryPlanMetadata getMetaData() {
		return metadata;
	}
	
	public DLAF getDLAF(){
		return this.dlaf;
	}
	
	public LAF getLAF() {
		return this.dlaf.getLAF();
	}

	public String getName() {
		return this.name;
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
	private static String generateName(String queryName) {
		candidateCount++;
		return queryName + "-QEP-" + candidateCount;
	}
}
