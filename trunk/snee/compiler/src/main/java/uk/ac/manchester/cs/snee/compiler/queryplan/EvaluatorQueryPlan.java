package uk.ac.manchester.cs.snee.compiler.queryplan;

import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;

/**
 * Query Plan for execution on SNEE Evaluator.
 */
public class EvaluatorQueryPlan extends QueryExecutionPlan {

	public EvaluatorQueryPlan(DLAF dlaf, String queryName) 
	throws SchemaMetadataException, TypeMappingException {
		super(dlaf, queryName);
	}
}
