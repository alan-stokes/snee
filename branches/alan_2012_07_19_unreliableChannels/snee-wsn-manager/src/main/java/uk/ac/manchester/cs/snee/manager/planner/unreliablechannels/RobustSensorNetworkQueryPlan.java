package uk.ac.manchester.cs.snee.manager.planner.unreliablechannels;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.queryplan.QueryPlanMetadata;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;

/**
 * Query Plan for Sensor Network that supports In-Network Query Processing.
 */
public class RobustSensorNetworkQueryPlan extends SensorNetworkQueryPlan {

  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -6153219169651756470L;

  /**
	 * Logger for this class.
	 */
	private final static Logger logger = Logger.getLogger(RobustSensorNetworkQueryPlan.class.getName());
	private LogicalOverlayNetworkHierarchy logicaloverlaynetwork;
	private UnreliableChannelAgenda overlayAgenda;

  /**
	 * Constructor
	 * @param dlaf The input DLAF
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 */
	public RobustSensorNetworkQueryPlan(SensorNetworkQueryPlan qep, 
	                                    LogicalOverlayNetworkHierarchy logicaloverlaynetwork, 
	                                    UnreliableChannelAgenda overlayAgenda) 
	throws  SchemaMetadataException, TypeMappingException 
	{
		super(qep.getDLAF(), qep.getRT(), qep.getDAF(), qep.getIOT(), qep.getAgenda(), qep.getQueryName());
		if (logger.isDebugEnabled())
			logger.debug("ENTER SensorNetworkQueryPlan()"); 
		this.logicaloverlaynetwork = logicaloverlaynetwork;
		this.overlayAgenda = overlayAgenda;
		
		SensornetOperator rootOperator = daf.getRootOperator();
		metadata = new QueryPlanMetadata(rootOperator.getAttributes());

		if (logger.isDebugEnabled())
			logger.debug("RETURN SensorNetworkQueryPlan()"); 
	}
	
	/**
	 * @return the logicla overlay for this QEP
	 */
	public LogicalOverlayNetworkHierarchy getLogicalOverlayNetwork()
	{
	  return this.logicaloverlaynetwork;
	}
	
	/**
	 * @return the agenda associated with the logical overlay
	 */
	public  UnreliableChannelAgenda getUnreliableAgenda()
	{
	  return this.overlayAgenda;
	}
}
