package uk.ac.manchester.cs.snee.compiler.planner;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.costmodels.cardinalitymodel.CardinalityEstimatedCostModel;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOTUtils;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.IOTUtils;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceWhereSchedular;
import uk.ac.manchester.cs.snee.compiler.params.qos.QoSExpectations;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAFUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.DLAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.EvaluatorQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAFUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlanAbstract;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.RTUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.sn.physical.AlgorithmSelector;
import uk.ac.manchester.cs.snee.compiler.sn.router.Router;
import uk.ac.manchester.cs.snee.compiler.sn.router.RouterException;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenScheduler;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenSchedulerException;
import uk.ac.manchester.cs.snee.compiler.sn.where.WhereScheduler;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;

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
	
	MetadataManager metadata;
	
	/**
	 * SourcePlanner constructor.
	 */
	public SourcePlanner (MetadataManager metadata) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER SourcePlanner()");
		this.metadata=metadata;
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
	 * @throws TypeMappingException
	 * @throws SNEEConfigurationException
	 * @throws OptimizationException 
	 * @throws WhenSchedulerException 
	 * @throws RouterException 
	 */
	public QueryExecutionPlanAbstract doSourcePlanning(DLAF dlaf, QoSExpectations qos, 
	CostParameters costParams, int queryID) 
	throws SNEEException, SchemaMetadataException, TypeMappingException, SNEEConfigurationException, 
	OptimizationException, WhenSchedulerException, RouterException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER doSourcePlanning() for " + queryID);
		QueryExecutionPlanAbstract qep = null;
		//TODO: In the future, this will involve iterating over fragment identified by source allocator in turn.
		SourceType dataSourceType = dlaf.getSources().iterator().next().getSourceType();
		switch (dataSourceType) {
		case SENSOR_NETWORK:
			qep = doSensorNetworkSourcePlanning(dlaf, qos, costParams, 
					"query"+queryID);
			break;
		case PULL_STREAM_SERVICE:
		case PUSH_STREAM_SERVICE:
		case QUERY_SERVICE:
		case UDP_SOURCE:
		case WSDAIR:
			qep = doEvaluatorPlanning(dlaf, queryID);
			break;
		default:
			String msg = "Unsupported data source type " + 
				dataSourceType;
			logger.warn(msg);
			throw new SNEEException(msg);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN doSourcePlanning() with " +
					qep.getID());
		}
		return qep;
	}

	/**
	 * Perform the query planning for a sensor network source.
	 * @param dlaf
	 * @param queryID
	 * @param costModel 
	 * @return
	 * @throws SNEEException
	 * @throws SchemaMetadataException
	 * @throws SNEEConfigurationException
	 * @throws OptimizationException 
	 * @throws WhenSchedulerException 
	 * @throws RouterException 
	 */
	private SensorNetworkQueryPlan doSensorNetworkSourcePlanning(DLAF dlaf,
	QoSExpectations qos, CostParameters costParams, String queryID) 
	throws SNEEException, TypeMappingException, SchemaMetadataException, 
	SNEEConfigurationException, OptimizationException, WhenSchedulerException, RouterException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER doSensorNetworkSourcePlanning() for " +
					queryID);
		logger.info("Starting Algorithm Selection for query " + queryID);
		PAF paf = doSNAlgorithmSelection(dlaf, qos, costParams, queryID);
		logger.info("Starting Routing for query " + queryID);		
		RT rt = doSNRouting(paf, queryID);
		logger.info("Starting Where-Scheduling for query " + queryID);
		InstanceWhereSchedular instanceWhere = new InstanceWhereSchedular(paf, rt, costParams);
		IOT iot = instanceWhere.getIOT();
		//DAF daf = instanceWhere.getDAF();
		//new DAFUtils(daf).generateGraphImage();
		//DAF daf = doSNWhereScheduling(rt, paf, costParams, queryID);
		//IOT iot = null;
		logger.info("Starting When-Scheduling for query " + queryID);
		//Agenda agenda = doSNWhenScheduling(daf, qos, queryID);
		//new AgendaUtils(agenda, false).exportAsLatex("agendaLatex");
		AgendaIOT agendaIOT = doSNWhenScheduling(iot, qos, queryID, costParams);
		new AgendaIOTUtils(agendaIOT, iot, true).generateImage();
		new AgendaIOTUtils(agendaIOT, iot, true).exportAsLatex();
		//agenda.setAgendaIOT(agendaIOT);
		SensorNetworkQueryPlan qep = new SensorNetworkQueryPlan(dlaf, rt, iot,
				agendaIOT, queryID, qos); //agenda		
		if (logger.isTraceEnabled())
			logger.trace("RETURN doSensorNetworkSourcePlanning()");
		return qep;
	}
	

	private AgendaIOT doSNWhenScheduling(IOT iot, QoSExpectations qos,
      String queryName, CostParameters costParams)
	  throws SNEEConfigurationException, SNEEException, SchemaMetadataException,
	  OptimizationException, WhenSchedulerException {
	    if (logger.isTraceEnabled())
	      logger.debug("ENTER doSNWhenScheduling()");
	    boolean useNetworkController = SNEEProperties.getBoolSetting(
	        SNEEPropertyNames.SNCB_INCLUDE_COMMAND_SERVER);
	    boolean allowDiscontinuousSensing = SNEEProperties.getBoolSetting(
	        SNEEPropertyNames.ALLOW_DISCONTINUOUS_SENSING);
	    WhenScheduler whenSched = new WhenScheduler(allowDiscontinuousSensing, metadata, useNetworkController);
	    AgendaIOT agenda = whenSched.doWhenScheduling(iot, qos, queryName, costParams);
	    if (SNEEProperties.getBoolSetting(SNEEPropertyNames.GENERATE_QEP_IMAGES)) {
	     // new AgendaIOTUtils(agenda, iot, true).generateImage();
	    }   
	    if (logger.isTraceEnabled())
	      logger.debug("RETURN doSNWhenScheduling()");
	    return agenda;
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
	private PAF doSNAlgorithmSelection(DLAF dlaf, QoSExpectations qos,
			CostParameters costParams, String queryName) 
	throws SNEEException, SchemaMetadataException, SNEEConfigurationException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER doSNAlgorithmSelection() for " + 
					queryName);
		AlgorithmSelector algorithmSelector = new AlgorithmSelector();
		PAF paf = algorithmSelector.doPhysicalOptimizaton(dlaf, qos, costParams, queryName);
		if (SNEEProperties.getBoolSetting(SNEEPropertyNames.GENERATE_QEP_IMAGES)) {
			new PAFUtils(paf).generateGraphImage();
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN doSNAlgorithmSelection()");
		return paf;
	}
	
	private RT doSNRouting(PAF paf, String queryName) 
	throws SNEEConfigurationException, RouterException
	{
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

	private DAF doSNWhereScheduling(RT rt, PAF paf, CostParameters costParams, 
	String queryID) 
	throws SNEEConfigurationException, SNEEException, SchemaMetadataException, 
	TypeMappingException, OptimizationException {
		if (logger.isTraceEnabled())
			logger.debug("ENTER doSNWhereScheduling()");
		
		boolean removeRedundantExchanges = true;
		if (SNEEProperties.isSet(SNEEPropertyNames.
				WHERE_SCHED_REMOVE_REDUNDANT_EXCHANGES)) {
			removeRedundantExchanges = SNEEProperties.getBoolSetting(SNEEPropertyNames.WHERE_SCHED_REMOVE_REDUNDANT_EXCHANGES);
		}
		
		WhereScheduler whereSched = new WhereScheduler(removeRedundantExchanges);
		DAF daf = whereSched.doWhereScheduling(paf, rt, costParams, queryID);
		if (SNEEProperties.getBoolSetting(SNEEPropertyNames.GENERATE_QEP_IMAGES)) {
		  logger.error("running daf generator ");
			new DAFUtils(daf).generateGraphImage();
		}		
		if (logger.isTraceEnabled())
			logger.debug("RETURN doSNWhereScheduling()");
		return daf;
	}

	private Agenda doSNWhenScheduling(DAF daf, QoSExpectations qos, String queryName) 
	throws SNEEConfigurationException, SNEEException, SchemaMetadataException,
	OptimizationException, WhenSchedulerException {
		if (logger.isTraceEnabled())
			logger.debug("ENTER doSNWhenScheduling()");
		boolean useNetworkController = SNEEProperties.getBoolSetting(
				SNEEPropertyNames.SNCB_INCLUDE_COMMAND_SERVER);
		boolean allowDiscontinuousSensing = SNEEProperties.getBoolSetting(
				SNEEPropertyNames.ALLOW_DISCONTINUOUS_SENSING);
		WhenScheduler whenSched = new WhenScheduler(allowDiscontinuousSensing,
				metadata, useNetworkController);
		Agenda agenda = whenSched.doWhenScheduling(daf, qos, queryName);
		if (SNEEProperties.getBoolSetting(SNEEPropertyNames.GENERATE_QEP_IMAGES)) {
			new AgendaUtils(agenda, true).generateImage();
		}		
		if (logger.isTraceEnabled())
			logger.debug("RETURN doSNWhenScheduling()");
		return agenda;
	}
	
	/**
	 * Invokes the evaluator query planning steps.
	 * @param dlaf
	 * @param queryName
	 * @return
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws SNEEException
	 * @throws SchemaMetadataException
	 * @throws SNEEConfigurationException
	 */
	private EvaluatorQueryPlan doEvaluatorPlanning(DLAF dlaf, int queryID) throws SchemaMetadataException, TypeMappingException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER doEvaluatorPlanning() for " + queryID);		
		EvaluatorQueryPlan qep = new EvaluatorQueryPlan(dlaf,
				"Q"+queryID);
		//TODO: In future, do physical optimization here, rather than in 
		//the evaluator as currently done
		if (logger.isTraceEnabled())
			logger.trace("RETURN doEvaluatorPlanning()");
		return qep;
	}
	
}
