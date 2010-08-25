package uk.ac.manchester.cs.snee.compiler.planner;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.SourceType;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAFUtils;
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
import uk.ac.manchester.cs.snee.compiler.sn.where.WhereScheduler;

/**
 * The SourcePlanner is responsible for carrying out query optimization 
 * specific to the type of source where the query is going to be evaluated.
 *
 */
public class SourcePlanner {

	/**
	 * Logger for this class.
	 */
	Logger logger = Logger.getLogger(this.getClass().getName());
	
	/**
	 * SourcePlanner constructor.
	 */
	public SourcePlanner () {
		if (logger.isDebugEnabled())
			logger.debug("ENTER SourcePlanner()");
		if (logger.isDebugEnabled())
			logger.debug("RETURN SourcePlanner()");
	}

	/**
	 * Perform the query planning specific to a particular type of source.
	 * @param queryID
	 * @param dlaf
	 * @return
	 * @throws SNEEException
	 * @throws SchemaMetadataException
	 * @throws SNEEConfigurationException
	 * @throws OptimizationException 
	 */
	public QueryExecutionPlan doSourcePlanning(int queryID, DLAF dlaf) 
	throws SNEEException, SchemaMetadataException, SNEEConfigurationException, 
	OptimizationException {
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

	/**
	 * Perform the query planning for a sensor network source.
	 * @param dlaf
	 * @param queryName
	 * @return
	 * @throws SNEEException
	 * @throws SchemaMetadataException
	 * @throws SNEEConfigurationException
	 * @throws OptimizationException 
	 */
	private SensorNetworkQueryPlan doSensorNetworkSourcePlanning(DLAF dlaf,
	String queryName) throws SNEEException, SchemaMetadataException, SNEEConfigurationException,
	OptimizationException {
		if (logger.isTraceEnabled())
			logger.debug("ENTER doSensorNetworkSourcePlanning()");
		//TODO: Add physical opt, routing, where- and when-scheduling here!		
		logger.info("Starting Algorithm Selection for query " + queryName);
		PAF paf = doSNAlgorithmSelection(dlaf,queryName);
		logger.info("Starting Routing for query " + queryName);		
		RT rt = doSNRouting(paf, queryName);
		logger.info("Starting Where-Scheduling for query " + queryName);
		DAF daf = doSNWhereScheduling(rt, paf, queryName);
		logger.info("Starting When-Scheduling for query " + queryName);
		//Agenda agenda = doWhenScheduling(rt, paf, queryName);
		SensorNetworkQueryPlan qep = new SensorNetworkQueryPlan(dlaf, 
				queryName); //agenda		
		if (logger.isTraceEnabled())
			logger.debug("RETURN doSensorNetworkSourcePlanning()");
		return qep;
	}
	

	/**
	 * Invokes the sensor network physical optimizer.
	 * @param dlaf
	 * @param queryName
	 * @return
	 * @throws SNEEException
	 * @throws SchemaMetadataException
	 * @throws SNEEConfigurationException
	 */
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
		return rt;
	}

	private DAF doSNWhereScheduling(RT rt, PAF paf, String queryName) 
	throws SNEEConfigurationException, SNEEException, SchemaMetadataException,
	OptimizationException {
		if (logger.isTraceEnabled())
			logger.debug("ENTER doSNWhereScheduling()");
		WhereScheduler whereSched = new WhereScheduler();
		DAF daf = whereSched.doWhereScheduling(paf, rt, queryName);
		if (SNEEProperties.getBoolSetting(SNEEPropertyNames.GENERATE_QEP_IMAGES)) {
			new DAFUtils(daf).generateGraphImage();
		}		
		if (logger.isTraceEnabled())
			logger.debug("RETURN doSNWhereScheduling()");
		return daf;
	}
	
	/**
	 * Invokes the evaluator query planning steps.
	 * @param dlaf
	 * @param queryName
	 * @return
	 * @throws SNEEException
	 * @throws SchemaMetadataException
	 * @throws SNEEConfigurationException
	 */
	private EvaluatorQueryPlan doEvaluatorPlanning(DLAF dlaf, int queryID) {
		if (logger.isTraceEnabled())
			logger.trace("ENTER doEvaluatorPlanning()");		
		EvaluatorQueryPlan qep = new EvaluatorQueryPlan(dlaf,
				"Q"+queryID);
		//TODO: In future, do physical optimization here, rather than in 
		//the evaluator as currently done
		if (logger.isTraceEnabled())
			logger.trace("RETURN doEvaluatorPlanning()");
		return qep;
	}
}
