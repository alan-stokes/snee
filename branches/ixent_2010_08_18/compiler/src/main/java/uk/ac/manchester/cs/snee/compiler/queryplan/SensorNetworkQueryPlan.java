package uk.ac.manchester.cs.snee.compiler.queryplan;

import org.apache.log4j.Logger;

/**
 * Query Plan for Sensor Network that supports In-Network Query Processing.
 */
public class SensorNetworkQueryPlan extends QueryExecutionPlan {

	/**
	 * Logger for this class.
	 */
	private Logger logger = Logger.getLogger(SensorNetworkQueryPlan.class.getName());
	
//	DAF daf;
//	
//	RT rt;
//	
//	Agenda agenda;
	
	/**
	 * Constructor
	 * @param dlaf The input DLAF
	 */
	public SensorNetworkQueryPlan(DLAF dlaf, String queryName) {
		super(dlaf, queryName);
		if (logger.isDebugEnabled())
			logger.debug("ENTER SensorNetworkQueryPlan()"); 
		if (logger.isDebugEnabled())
			logger.debug("RETURN SensorNetworkQueryPlan()"); 
	}

//	
//	protected SensorNetworkQueryPlan(DAF daf, Rt rt. Agenda agenda) {
//		super(daf.getDLAF());
//	}

	
	
	
}
