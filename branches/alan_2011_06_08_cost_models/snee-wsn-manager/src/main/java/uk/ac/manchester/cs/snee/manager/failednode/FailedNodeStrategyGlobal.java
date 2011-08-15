package uk.ac.manchester.cs.snee.manager.failednode;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.rits.cloning.Cloner;

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
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.sn.router.Router;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenScheduler;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenSchedulerException;
import uk.ac.manchester.cs.snee.manager.Adaptation;
import uk.ac.manchester.cs.snee.manager.AutonomicManager;
import uk.ac.manchester.cs.snee.manager.StrategyAbstract;
import uk.ac.manchester.cs.snee.manager.StrategyID;
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

public class FailedNodeStrategyGlobal extends StrategyAbstract 
{
  private IOT oldIOT;
  private AgendaIOT oldAgenda;
  private MetadataManager _metadataManager;
  private Topology network;
  private Cloner cloner;
  private File globalFile;
  
  
	public FailedNodeStrategyGlobal(AutonomicManager manager, 
	                                 SourceMetadataAbstract _metadata, 
	                                 MetadataManager _metadataManager)
  {
    super(manager, _metadata);
    this._metadataManager = _metadataManager;
    globalFile = new File(outputFolder.toString() + sep + "global Stragety");
    globalFile.mkdir();
    cloner = new Cloner();
    cloner.dontClone(Logger.class);
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
	public List<Adaptation> adapt(ArrayList<String> failedNodes)
	throws 
	OptimizationException, UnsupportedAttributeTypeException,
	SchemaMetadataException,SourceMetadataException,
	TypeMappingException,AgendaException, 
	SNEEException,SNEEConfigurationException, 
	MalformedURLException,TopologyReaderException,
	MetadataException, SNEEDataSourceException,
	CostParametersException, SNCBException, NumberFormatException, SNEECompilerException 
	
	{
	  System.out.println("Running Failed Node FrameWork Global");
	  List<Adaptation> adaptation = new ArrayList<Adaptation>();
	  Adaptation adapt = new Adaptation(qep, StrategyID.FAILED_NODE_GLOBAL);
	  //remove nodes from topology
		network = this.getWsnTopology();
		network = cloner.deepClone(network);
		Iterator<String> failedNodeIterator = failedNodes.iterator();
		while(failedNodeIterator.hasNext())
		{
		  String nodeID = failedNodeIterator.next();
		  network.removeNode(nodeID);
		}
		//remove exchanges from PAF
		PAF paf = cloner.deepClone(qep.getIOT().getPAF());
		paf = this.removeExchangesFromPAF(paf);
		//shove though distributed section of compiler
		//routing
		Router router = new Router();
		RT routingTree = router.doRouting(paf, qep.getQueryName(), network);
		//where
		InstanceWhereSchedular instanceWhere = 
		  new InstanceWhereSchedular(paf, routingTree, qep.getCostParameters(), 
		                             globalFile.toString());
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
