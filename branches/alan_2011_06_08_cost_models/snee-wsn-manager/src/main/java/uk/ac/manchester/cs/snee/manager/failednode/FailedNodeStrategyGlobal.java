package uk.ac.manchester.cs.snee.manager.failednode;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
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
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOTUtils;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceWhereSchedular;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.sn.router.Router;
import uk.ac.manchester.cs.snee.compiler.sn.router.RouterException;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenScheduler;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenSchedulerException;
import uk.ac.manchester.cs.snee.manager.AutonomicManager;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.StrategyIDEnum;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

public class FailedNodeStrategyGlobal extends FailedNodeStrategyAbstract 
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -4443257551168408228L;
  private MetadataManager _metadataManager;
  private Topology network;
  private File globalFile;
  
  
	public FailedNodeStrategyGlobal(AutonomicManager manager, 
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
	  this.currentQEP = (SensorNetworkQueryPlan) oldQep;
	  outputFolder = manager.getOutputFolder();
    this.currentQEP.getIOT().setID("OldIOT");
	}

	/**
	 * removes failed nodes from the network and then calls upon distributed section of compiler.
	 * @throws SNEECompilerException 
	 * @throws RouterException 
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
	  this.currentQEP = (SensorNetworkQueryPlan) manager.getCurrentQEP();
	  System.out.println("Running Failed Node FrameWork Global");
    globalFile = new File(outputFolder.toString() + sep + "global Stragety");
    globalFile.mkdir();
	  List<Adaptation> adaptation = new ArrayList<Adaptation>();
	  Adaptation adapt = new Adaptation(currentQEP, StrategyIDEnum.FailedNodeGlobal, 1);
		SensorNetworkSourceMetadata sm = (SensorNetworkSourceMetadata) _metadata;
		network = sm.getTopology();
		
	  makeNetworkFile();
		//remove exchanges from PAF
		PAF paf = cloner.deepClone(currentQEP.getIOT().getPAF());
		paf.updateMetadataConnection(sm);
		paf = this.removeExchangesFromPAF(paf);
		//shove though distributed section of compiler
		//routing
		Router router = new Router();
		RT routingTree;
		//if no route generated, then return empty adapatation.
    try
    {
      routingTree = router.doRouting(paf, currentQEP.getQueryName(), network, _metadata);
    }
    catch (RouterException e1)
    {
      return adaptation;
    }
		//where
		InstanceWhereSchedular instanceWhere = 
		  new InstanceWhereSchedular(paf, routingTree, currentQEP.getCostParameters(), 
		                             globalFile.toString());
    IOT newIOT = instanceWhere.getIOT();
    //when
    boolean useNetworkController = SNEEProperties.getBoolSetting(
        SNEEPropertyNames.SNCB_INCLUDE_COMMAND_SERVER);
    boolean allowDiscontinuousSensing = SNEEProperties.getBoolSetting(
        SNEEPropertyNames.ALLOW_DISCONTINUOUS_SENSING);
    AgendaIOT newAgendaIOT;
    Agenda newAgenda;
    WhenScheduler whenSched = new WhenScheduler(allowDiscontinuousSensing, 
                                                _metadataManager.getCostParameters(), 
                                                useNetworkController);
    try
    {
      newAgendaIOT = whenSched.doWhenScheduling(newIOT, currentQEP.getQos(), currentQEP.getQueryName(), currentQEP.getCostParameters());
      newAgenda = whenSched.doWhenScheduling(newIOT.getDAF(), currentQEP.getQos(), currentQEP.getQueryName());
      new AgendaIOTUtils(newAgendaIOT, newIOT, false).exportAsLatex(globalFile.toString() + sep + "newAgenda");
      new AgendaIOTUtils(newAgendaIOT, newIOT, false).generateImage(globalFile.toString());
      //new AgendaIOTUtils(newAgenda, newIOT, false).generateImage(globalFile.toString() + sep + "newAgenda");
    }
    catch (WhenSchedulerException e)
    {
      throw new SNEECompilerException(e);
    }
    
    boolean success = assessQEPsAgendas(this.currentQEP.getIOT(), newIOT, this.currentQEP.getAgendaIOT(), newAgendaIOT, newAgenda, false, adapt, failedNodes, routingTree);
    adapt.setFailedNodes(failedNodes);
    
    if(success)
      adaptation.add(adapt);
		return adaptation;
	}

  private void makeNetworkFile() 
  throws SchemaMetadataException
  {
    File top = new File(globalFile.toString() + sep + "topology");
    top.mkdir();
    network.exportAsDOTFile(top.toString() + sep + "topology");  
  }

}
