package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.params.qos.QoSExpectations;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;
import uk.ac.manchester.cs.snee.sncb.SNCB;

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
	private AgendaIOT agendaIOT;
	
	private IOT iot;
	
	private QoSExpectations qos = null;

  /**
	 * Constructor
	 * @param dlaf The input DLAF
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 */
	public SensorNetworkQueryPlan(DLAF dlaf, RT rt, DAF daf, IOT iot, Agenda agenda, 
	String queryName) 
	throws  SchemaMetadataException, TypeMappingException {
		super(dlaf, queryName);
		if (logger.isDebugEnabled())
			logger.debug("ENTER SensorNetworkQueryPlan()"); 
		this.rt = rt;
		this.daf = daf;
		this.agenda = agenda;
		this.iot = iot;
		
		SensornetOperator rootOperator = daf.getRootOperator();
		metadata = new QueryPlanMetadata(rootOperator.getAttributes());

		if (logger.isDebugEnabled())
			logger.debug("RETURN SensorNetworkQueryPlan()"); 
	}
	
  /**
   * Constructor
   * @param dlaf The input DLAF
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   */
  public SensorNetworkQueryPlan(DLAF dlaf, RT rt, DAF daf, IOT iot, AgendaIOT agenda, 
  String queryName) 
  throws  SchemaMetadataException, TypeMappingException {
    super(dlaf, queryName);
    if (logger.isDebugEnabled())
      logger.debug("ENTER SensorNetworkQueryPlan()"); 
    this.rt = rt;
    this.daf = daf;
    this.agendaIOT = agenda;
    this.iot = iot;
    
    SensornetOperator rootOperator = daf.getRootOperator();
    metadata = new QueryPlanMetadata(rootOperator.getAttributes());

    if (logger.isDebugEnabled())
      logger.debug("RETURN SensorNetworkQueryPlan()"); 
  }
  
	public SensorNetworkQueryPlan(DLAF dlaf, RT rt, DAF daf, IOT iot,
      Agenda agenda, String queryName, QoSExpectations qos) 
	throws SchemaMetadataException, TypeMappingException
  {
	  super(dlaf, queryName);
    if (logger.isDebugEnabled())
      logger.debug("ENTER SensorNetworkQueryPlan()"); 
    this.rt = rt;
    this.daf = daf;
    this.agenda = agenda;
    this.iot = iot;
    this.qos = qos;
    
    SensornetOperator rootOperator = daf.getRootOperator();
    metadata = new QueryPlanMetadata(rootOperator.getAttributes());

    if (logger.isDebugEnabled())
      logger.debug("RETURN SensorNetworkQueryPlan()"); 
  }

  public SensorNetworkQueryPlan(DLAF dlaf, RT rt, IOT iot,
      AgendaIOT agendaIOT, String queryName, QoSExpectations qos)
  throws SchemaMetadataException, TypeMappingException
  {
    super(dlaf, queryName);
    if (logger.isDebugEnabled())
      logger.debug("ENTER SensorNetworkQueryPlan()"); 
    this.rt = rt;
    this.agendaIOT = agendaIOT;
    this.daf = iot.getDAF();
    this.agenda = null;
    this.iot = iot;
    this.qos = qos;
    
    SensornetOperator rootOperator = iot.getRoot().getSensornetOperator();
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
