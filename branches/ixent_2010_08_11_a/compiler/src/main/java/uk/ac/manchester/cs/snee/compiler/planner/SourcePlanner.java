package uk.ac.manchester.cs.snee.compiler.planner;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceType;
import uk.ac.manchester.cs.snee.compiler.queryplan.DLAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.EvaluatorQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;

public class SourcePlanner {

	Logger logger = Logger.getLogger(this.getClass().getName());
	
	public SourcePlanner () 
	{
		if (logger.isDebugEnabled())
			logger.debug("ENTER SourcePlanner()");
		if (logger.isDebugEnabled())
			logger.debug("RETURN SourcePlanner()");
	}

	public QueryExecutionPlan doSourcePlanning(DLAF dlaf) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER doSourcePlanning()");
		//TODO: In the future, this will involve iterating over fragment 
		//identified by source allocator in turn.
		if (dlaf.getSourceType()==SourceType.SENSOR_NETWORK) {
			SensorNetworkQueryPlan qep = doSensorNetworkSourcePlanning(dlaf);
			if (logger.isDebugEnabled())
				logger.debug("RETURN doSourcePlanning()");
			return qep;
		} else {
			EvaluatorQueryPlan qep = doEvaluatorPlanning(dlaf);
			if (logger.isDebugEnabled())
				logger.debug("RETURN doSourcePlanning()");
			return qep;
		}
	}

	private SensorNetworkQueryPlan doSensorNetworkSourcePlanning(DLAF dlaf) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER doSensorNetworkSourcePlanning()");		
		SensorNetworkQueryPlan qep = new SensorNetworkQueryPlan(dlaf);
		//TODO: Add physical opt, routing, where- and when-scheduling here!
		
		// Physical Optimisation
		//		logger.info("Starting Physical Optimization");
		//		PAF paf = PhysicalOptimization.doPhysicalOptimization(
		//				laf, 
		//				queryName);
		//		return paf;
		if (logger.isDebugEnabled())
			logger.debug("RETURN doSensorNetworkSourcePlanning()");
		return qep;
	}
	
	private EvaluatorQueryPlan doEvaluatorPlanning(DLAF dlaf) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER doEvaluatorPlanning()");		
		EvaluatorQueryPlan qep = new EvaluatorQueryPlan(dlaf);
		//TODO: In future, do physical optimization here, rather than in 
		//the evaluator as currently done
		if (logger.isDebugEnabled())
			logger.debug("RETURN doEvaluatorPlanning()");
		return qep;
	}
	
	
}
