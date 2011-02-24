package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.util.Iterator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
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
	
	/**
	 * Constructor
	 * @param dlaf The input DLAF
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 */
	public SensorNetworkQueryPlan(DLAF dlaf, RT rt, DAF daf, Agenda agenda, 
	String queryName) 
	throws  SchemaMetadataException, TypeMappingException {
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
	 * @return the agenda
	 */
	public Agenda getAgenda() {
		return agenda;
	}

	public long getAcquisitionInterval_ms() {
		return this.agenda.getAcquisitionInterval_ms();
	}
	
	public long getAcquisitionInterval_bms() {
		return this.agenda.getAcquisitionInterval_bms();
	}
	
	public long getBufferingFactor() {
		return this.agenda.getBufferingFactor();
	}

	//delegate
	public Iterator<Site> siteIterator(TraversalOrder order) {
		return this.getRT().siteIterator(order);
	}

	public int getGateway() {
		return new Integer(this.getRT().getRoot().getID()).intValue();
	}

	public CostParameters getCostParameters() {
		return this.getAgenda().getCostParameters();
	}

	public SNCB getSNCB() {
		//XXX: This works only because there is one sensor network assumed.
		SensorNetworkSourceMetadata metadata = 
			(SensorNetworkSourceMetadata)this.dlaf.getSource();
		return metadata.getSNCB();
	}

	
//	
//	protected SensorNetworkQueryPlan(DAF daf, Rt rt. Agenda agenda) {
//		super(daf.getDLAF());
//	}

	
	
	
}
