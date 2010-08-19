package uk.ac.manchester.cs.snee.compiler.planner;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceType;
import uk.ac.manchester.cs.snee.compiler.queryplan.DLAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.DLAFUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.EvaluatorQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAFUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.RTUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.sn.physical.AlgorithmSelector;
import uk.ac.manchester.cs.snee.compiler.sn.router.Router;

/**
 * The SourcePlanner is responsible for carrying out query optimization 
 * specific to the type of source where the query is going to be evaluated.
 *
 */
public class SourcePlanner {

	Logger logger = Logger.getLogger(this.getClass().getName());
	
	public SourcePlanner () 
	{
		if (logger.isDebugEnabled())
			logger.debug("ENTER SourcePlanner()");
		if (logger.isDebugEnabled())
			logger.debug("RETURN SourcePlanner()");
	}

	public QueryExecutionPlan doSourcePlanning(int queryID, DLAF dlaf) 
	throws SNEEException, SchemaMetadataException, SNEEConfigurationException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER doSourcePlanning()");
		//TODO: In the future, this will involve iterating over fragment 
		//identified by source allocator in turn.
		logger.info("Only source="+dlaf.getSource().getSourceName());
		if (dlaf.getSourceType()==SourceType.SENSOR_NETWORK) {
			SensorNetworkQueryPlan qep = doSensorNetworkSourcePlanning(dlaf,
					"query"+queryID);
			if (logger.isDebugEnabled())
				logger.debug("RETURN doSourcePlanning()");
			return qep;
		} else {
			EvaluatorQueryPlan qep = doEvaluatorPlanning(dlaf, queryID);
			if (logger.isDebugEnabled())
				logger.debug("RETURN doSourcePlanning()");
			return qep;
		}
	}

	private SensorNetworkQueryPlan doSensorNetworkSourcePlanning(DLAF dlaf,
	String queryName) throws SNEEException, SchemaMetadataException, SNEEConfigurationException {
		if (logger.isTraceEnabled())
			logger.debug("ENTER doSensorNetworkSourcePlanning()");
		//TODO: Add physical opt, routing, where- and when-scheduling here!		
		if (logger.isInfoEnabled()) 
			logger.info("Starting Algorithm Selection for query " + queryName);
		PAF paf = doSNAlgorithmSelection(dlaf,queryName);
		if (logger.isInfoEnabled()) 
			logger.info("Starting Routing for query " + queryName);		
		RT rt = doSNRouting(paf, queryName);
		if (logger.isInfoEnabled()) 
			logger.info("Starting Where-Scheduling for query " + queryName);
		//DAF daf = doWhereScheduling(rt, paf, queryName);
		if (logger.isInfoEnabled()) 
			logger.info("Starting When-Scheduling for query " + queryName);
		//Agenda agenda = doWhenScheduling(rt, paf, queryName);
		SensorNetworkQueryPlan qep = new SensorNetworkQueryPlan(dlaf, 
				queryName); //agenda		
		if (logger.isTraceEnabled())
			logger.debug("RETURN doSensorNetworkSourcePlanning()");
		return qep;
	}
	

	private PAF doSNAlgorithmSelection(DLAF dlaf, String queryName) 
	throws SNEEException, SchemaMetadataException, SNEEConfigurationException {
		if (logger.isTraceEnabled())
			logger.debug("ENTER doSNAlgorithmSelection()");
		AlgorithmSelector algorithmSelector = new AlgorithmSelector();
		PAF paf = algorithmSelector.doPhysicalOptimizaton(dlaf, queryName);
		if (SNEEProperties.getBoolSetting(SNEEPropertyNames.GENERATE_QEP_IMAGES)) {
			new PAFUtils(paf).generateGraphImage();
		}
		if (logger.isTraceEnabled())
			logger.debug("RETURN doSNAlgorithmSelection()");
		return paf;
	}
	
	private RT doSNRouting(PAF paf, String queryName) throws SNEEConfigurationException {
		if (logger.isTraceEnabled())
			logger.debug("ENTER doSNRouting()");
		Router router = new Router();
		RT rt = router.doRouting(paf, queryName);
		if (SNEEProperties.getBoolSetting(SNEEPropertyNames.GENERATE_QEP_IMAGES)) {
			new RTUtils(rt).generateGraphImage();
		}
		if (logger.isTraceEnabled())
			logger.debug("RETURN doSNRouting()");
		return null;
	}
	private EvaluatorQueryPlan doEvaluatorPlanning(DLAF dlaf, int queryID) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER doEvaluatorPlanning()");		
		EvaluatorQueryPlan qep = new EvaluatorQueryPlan(dlaf,
				"Q"+queryID);
		//TODO: In future, do physical optimization here, rather than in 
		//the evaluator as currently done
		if (logger.isDebugEnabled())
			logger.debug("RETURN doEvaluatorPlanning()");
		return qep;
	}
}
