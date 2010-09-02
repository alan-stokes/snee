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
	
	private DAF daf;
	
	private RT rt;
	
	private Agenda agenda;
	
	/**
	 * Constructor
	 * @param dlaf The input DLAF
	 */
	public SensorNetworkQueryPlan(DLAF dlaf, RT rt, DAF daf, Agenda agenda, 
	String queryName) {
		super(dlaf, queryName);
		if (logger.isDebugEnabled())
			logger.debug("ENTER SensorNetworkQueryPlan()"); 
		this.rt = rt;
		this.daf = daf;
		this.agenda = agenda;
		if (logger.isDebugEnabled())
			logger.debug("RETURN SensorNetworkQueryPlan()"); 
	}

	/**
	 * @return the daf
	 */
	public DAF getDaf() {
		return daf;
	}

	/**
	 * @return the rt
	 */
	public RT getRt() {
		return rt;
	}

	/**
	 * @return the agenda
	 */
	public Agenda getAgenda() {
		return agenda;
	}

	
//	
//	protected SensorNetworkQueryPlan(DAF daf, Rt rt. Agenda agenda) {
//		super(daf.getDLAF());
//	}

	
	
	
}
