package uk.ac.manchester.cs.snee.manager.failednode;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceWhereSchedular;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.sn.router.Router;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenScheduler;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenSchedulerException;
import uk.ac.manchester.cs.snee.manager.Adapatation;
import uk.ac.manchester.cs.snee.manager.AutonomicManager;
import uk.ac.manchester.cs.snee.manager.FrameWorkAbstract;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

public class FailedNodeFrameWorkGlobal extends FrameWorkAbstract 
{
  private IOT oldIOT;
  private AgendaIOT oldAgenda;
  private MetadataManager _metadataManager;
  
  
	public FailedNodeFrameWorkGlobal(AutonomicManager manager, 
	                                 SourceMetadataAbstract _metadata, 
	                                 MetadataManager _metadataManager)
  {
    super(manager, _metadata);
    this._metadataManager = _metadataManager;
  }

  @Override
	public boolean canAdapt(String failedNode) 
	{
		return true;
	}

	@Override
	public boolean canAdaptToAll(ArrayList<String> failedNodes)
	{
		return true;
	}

	@Override
	public void initilise(QueryExecutionPlan oldQep, Integer noTrees)
	throws SchemaMetadataException 
	{
	  this.qep = (SensorNetworkQueryPlan) oldQep;
	  outputFolder = manager.getOutputFolder();
	  this.oldIOT = qep.getIOT();
    oldIOT.setID("OldIOT");
    this.oldAgenda = this.qep.getAgendaIOT();
	}

	/**
	 * removes failed nodes from the network and then calls upon distributed section of compiler.
	 * @throws SNEECompilerException 
	 */
	@Override
	public List<Adapatation> adapt(ArrayList<String> failedNodes)
	throws 
	OptimizationException, UnsupportedAttributeTypeException,
	SchemaMetadataException,SourceMetadataException,
	TypeMappingException,AgendaException, 
	SNEEException,SNEEConfigurationException, 
	MalformedURLException,TopologyReaderException,
	MetadataException, SNEEDataSourceException,
	CostParametersException, SNCBException, NumberFormatException, SNEECompilerException 
	
	{
	  List<Adapatation> adaptation = new ArrayList<Adapatation>();
	  Adapatation adapt = new Adapatation(qep);
	  //remove nodes from topology
		Topology network = this.getWsnTopology();
		Iterator<String> failedNodeIterator = failedNodes.iterator();
		while(failedNodeIterator.hasNext())
		{
		  String nodeID = failedNodeIterator.next();
		  network.removeNode(nodeID);
		}
		//shove though distributed section of compiler
		//routing
		Router router = new Router();
		RT routingTree = router.doRouting(qep.getIOT().getPAF(), qep.getQueryName());
		//where
		InstanceWhereSchedular instanceWhere = 
		  new InstanceWhereSchedular(qep.getIOT().getPAF(), routingTree, qep.getCostParameters());
    IOT newIOT = instanceWhere.getIOT();
    //when
    boolean useNetworkController = SNEEProperties.getBoolSetting(
        SNEEPropertyNames.SNCB_INCLUDE_COMMAND_SERVER);
    boolean allowDiscontinuousSensing = SNEEProperties.getBoolSetting(
        SNEEPropertyNames.ALLOW_DISCONTINUOUS_SENSING);
    AgendaIOT newAgenda;
    WhenScheduler whenSched = new WhenScheduler(allowDiscontinuousSensing, _metadataManager, useNetworkController);
    try
    {
      newAgenda = whenSched.doWhenScheduling(newIOT, qep.getQos(), qep.getQueryName(), qep.getCostParameters());
    }
    catch (WhenSchedulerException e)
    {
      throw new SNEECompilerException(e);
    }
    
    boolean success = assessQEPsAgendas(oldIOT, newIOT, oldAgenda, newAgenda, false, adapt, failedNodes, routingTree);
    if(success)
      adaptation.add(adapt);
		return adaptation;
	}

}
