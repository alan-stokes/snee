package uk.ac.manchester.cs.snee.manager.planner.unreliablechannels;

import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.params.qos.QoSExpectations;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.DLAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlanAbstract;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryPlanMetadata;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.logicaloverlaynetworkgenerator.LogicalOverlayNetwork;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;
import uk.ac.manchester.cs.snee.sncb.SNCB;

/**
 * Query Plan for Sensor Network that supports In-Network Query Processing.
 */
public class RobustSensorNetworkQueryPlan extends QueryExecutionPlanAbstract {

  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -6153219169651756470L;

  /**
	 * Logger for this class.
	 */
	private final static Logger logger = Logger.getLogger(RobustSensorNetworkQueryPlan.class.getName());
	
	private DAF daf;
	
	private RT rt;
	
	private Agenda agenda;
	private AgendaIOT agendaIOT;
	private LogicalOverlayNetwork logicaloverlaynetwork;
	private UnreliableChannelAgenda overlayAgenda;
	private IOT iot;
	
	private QoSExpectations qos = null;

  /**
	 * Constructor
	 * @param dlaf The input DLAF
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 */
	public RobustSensorNetworkQueryPlan(SensorNetworkQueryPlan qep, 
	                                    LogicalOverlayNetwork logicaloverlaynetwork, 
	                                    UnreliableChannelAgenda overlayAgenda) 
	throws  SchemaMetadataException, TypeMappingException {
		super(qep.getDLAF(), qep.getQueryName());
		if (logger.isDebugEnabled())
			logger.debug("ENTER SensorNetworkQueryPlan()"); 
		this.rt = qep.getRT();
		this.daf = qep.getDAF();
		this.agenda = qep.getAgenda();
		this.iot = qep.getIOT();
		this.logicaloverlaynetwork = logicaloverlaynetwork;
		this.overlayAgenda = overlayAgenda;
		
		SensornetOperator rootOperator = daf.getRootOperator();
		metadata = new QueryPlanMetadata(rootOperator.getAttributes());

		if (logger.isDebugEnabled())
			logger.debug("RETURN SensorNetworkQueryPlan()"); 
	}

  /**
	 * @return the daf
	 */
	public DAF getDAF() {
		return daf;
	}

	/**
	 * @return the rt
	 */
	public RT getRT() {
		return rt;
	}
	
	/**
	 * @return the logicla overlay for this QEP
	 */
	public LogicalOverlayNetwork getLogicalOverlayNetwork()
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

	/**
	 * @return the instance daf
	 */
	public IOT getIOT() {
		return iot;
	}

	/**
	 * @return the agenda
	 */
	public Agenda getAgenda() {
		return agenda;
	}

	public AgendaIOT getAgendaIOT()
	{
	  return agendaIOT;
	}
	public long getAcquisitionInterval_ms() {
	  if(this.agenda != null)
		  return this.agenda.getAcquisitionInterval_ms();
	  else
	    return this.agendaIOT.getAcquisitionInterval_ms();
	}
	
	public long getAcquisitionInterval_bms() {
	  if(this.agenda != null) 
		  return this.agenda.getAcquisitionInterval_bms();
	  else
      return this.agendaIOT.getAcquisitionInterval_bms();
	}
	
	public long getBufferingFactor() {
	  if(this.agenda != null) 
		  return this.agenda.getBufferingFactor();
	  else
	    return this.agendaIOT.getBufferingFactor();
	}

	//delegate
	public Iterator<Site> siteIterator(TraversalOrder order) {
		return this.getRT().siteIterator(order);
	}

	public int getGateway() {
		return new Integer(this.getRT().getRoot().getID()).intValue();
	}

	public CostParameters getCostParameters() {
	  if(this.getAgenda() != null)
		  return this.getAgenda().getCostParameters();
	  else
	    return this.getAgendaIOT().getCostParameters();
	}

	public SNCB getSNCB() {
		//XXX: This works only because there is one sensor network assumed.
		Set<SourceMetadataAbstract> sources = dlaf.getSources();
		SensorNetworkSourceMetadata metadata = 
			(SensorNetworkSourceMetadata) sources.iterator().next();
		return metadata.getSNCB();
	}
	
	 public QoSExpectations getQos()
	  {
	    return qos;
	  }

	  public void setQos(QoSExpectations qos)
	  {
	    this.qos = qos;
	  }

	
//	
//	protected SensorNetworkQueryPlan(DAF daf, Rt rt. Agenda agenda) {
//		super(daf.getDLAF());
//	}

	
	
	
}
