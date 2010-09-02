package uk.ac.manchester.cs.snee.compiler.queryplan;

import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;

/**
 * Query Plan for Sensor Network that supports In-Network Query Processing.
 */
public class SensorNetworkQueryPlan extends QueryExecutionPlan {

//	DAF daf;
//	
//	RT rt;
//	
//	Agenda agenda;
	
	/**
	 * Constructor
	 * @param dlaf The input DLAF
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 */
	public SensorNetworkQueryPlan(DLAF dlaf, String queryName) 
	throws SchemaMetadataException, TypeMappingException {
		super(dlaf, queryName);
	}

//	
//	protected SensorNetworkQueryPlan(DAF daf, Rt rt. Agenda agenda) {
//		super(daf.getDLAF());
//	}

	
	
	
}
