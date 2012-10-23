package uk.ac.manchester.cs.snee.manager.failednode;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import com.rits.cloning.Cloner;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.WhenSchedulerException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOTUtils;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceWhereSchedular;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.RTUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.sn.router.Router;
import uk.ac.manchester.cs.snee.compiler.sn.router.RouterException;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenScheduler;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.common.StrategyIDEnum;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;
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
  private ArrayList<String> nodesFailed = new ArrayList<String>();
  private ArrayList<Integer> lifetimes = new ArrayList<Integer>();
  
  
	public FailedNodeStrategyGlobal(AutonomicManagerImpl manager, 
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
	  new File("results").mkdir();
	  outputFolder = manager.getOutputFolder();
    this.currentQEP.getIOT().setID("OldIOT");
    try{ 
      BufferedWriter out = new BufferedWriter(new FileWriter("results/results.tex", true));
      out.write("Q" + manager.getQueryName() + "global est lifetime is " + estimateOverallLifetime(manager.getQueryName()) + " with nodes "+ stringOutput() + " \n");
      out.flush();
      out.close();
      
      nodesFailed.clear();
      }
    catch(Exception e)
    {
      e.printStackTrace();
    }
	}

	private String stringOutput()
  {
    String output = "";
    Iterator<String> nodesFailedIterator = nodesFailed.iterator();
    while(nodesFailedIterator.hasNext())
    {
      output = output.concat(nodesFailedIterator.next() + " ");
    }
    output = output.concat("lifetimes are" + " ");
    Iterator<Integer> lifetimesIterator = lifetimes.iterator();
    while(lifetimesIterator.hasNext())
    {
      output = output.concat(lifetimesIterator.next().toString() + " ");
    }
    return output;
  }

  private Integer estimateOverallLifetime(String queryname) 
	throws FileNotFoundException, IOException, OptimizationException, 
	SchemaMetadataException, TypeMappingException, SNEEConfigurationException,
	NumberFormatException, UnsupportedAttributeTypeException,
	SourceMetadataException, AgendaException, SNEEException,
	TopologyReaderException, MetadataException, SNEEDataSourceException, 
	CostParametersException, SNCBException, SNEECompilerException, CodeGenerationException
  {
	  int lifetime = 0;
	  Cloner cloner = new Cloner();
	  cloner.dontClone(Logger.class);
	  
	  HashMap<String, RunTimeSite> runningSites = manager.getCopyOfRunningSites();
	  SensorNetworkSourceMetadata sm = (SensorNetworkSourceMetadata) _metadata;
    network = sm.getTopology();
    int counter = 0;
	  SensorNetworkQueryPlan qep = this.currentQEP;
	  setupRunningSites(qep, runningSites);
	  while(true)
	  {
	    ArrayList<Integer> results = manager.locateNextNodeFailureAndTimeFromEnergyDepletion(runningSites, qep);
  	  ArrayList<String> failedNodes = new ArrayList<String>();
  	  failedNodes.add(results.get(1).toString());
  	  nodesFailed.add(results.get(1).toString());
  	  lifetimes.add(results.get(0));
  	  
  	  //fix bug where wont stop when acq node fialed
  	  Integer[] sources = sm.getSourceSites();
  	  boolean located = false;
  	  int index = 0;
  	  while(index < sources.length && !located)
  	  {
  	    if(sources[index] == results.get(1))
  	      located = true;
  	    index++;
  	  }
  	  if(located)
  	  {
  	    networkEnergyReport(runningSites, new File("results/qep" + queryname + " " + counter + "energy"), results.get(0));
  	    return lifetime + results.get(0);
  	  }
  	  
  	  network.removeNodeAndAssociatedEdges(results.get(1).toString());
  	  runningSites.get(results.get(1).toString()).setQepExecutionCost(0.0);
  	  List<Adaptation> adaptations = this.adapt(failedNodes);
  	  if(adaptations.size() == 0)
  	  {
  	    networkEnergyReport(runningSites, new File("results/qep" + queryname + " " + counter + "energy"), results.get(0));
  	    return lifetime + results.get(0);
  	  }
  	  else
  	  {
  	    lifetime += results.get(0);
  	    removeEnergyLevels(runningSites, qep, results.get(0));
  	    manager.assessChoice(adaptations.get(0), runningSites);
  	    qep = adaptations.get(0).getNewQep();
  	    setupRunningSites(qep, runningSites);
  	    new RTUtils(qep.getRT()).exportAsDotFile("results/qep" + queryname + " " + counter);
  	    networkEnergyReport(runningSites, new File("results/qep" + queryname + " " + counter + "energy"), results.get(0));
  	    counter++;
  	  }
	  }
  }
  
  /**
   * ouputs the energies left by the network once the qep has failed
   * @param successor
   */
  private void networkEnergyReport(HashMap<String, RunTimeSite> runningSites, 
                                  File successorFolder, Integer lifetime)
  {
    try 
    {
      final PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(successorFolder)));
      Iterator<String> keys = runningSites.keySet().iterator();
      while(keys.hasNext())
      {
        String key = keys.next();
        RunTimeSite site = runningSites.get(key);
        Double cost = site.getQepExecutionCost() * lifetime;
        Double leftOverEnergy = site.getCurrentEnergy() - cost;
        out.println("Node " + key + " has residual energy " + 
                    leftOverEnergy + " and had energy of " + site.getCurrentEnergy() + 
                    " and qep Cost of " + site.getQepExecutionCost()) ; 
      }
      out.flush();
      out.close();
    }
    catch(Exception e)
    {
      System.out.println("couldnt write the energy report");
    }
  }
	
  private void setupRunningSites(SensorNetworkQueryPlan qep,
                                 HashMap<String, RunTimeSite> runningSites)
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    Iterator<Node> siteIterator = this.getWsnTopology().getNodes().iterator();
    while(siteIterator.hasNext())
    {
      Site currentSite = (Site) siteIterator.next();
      Double qepExecutionCost = qep.getAgendaIOT().getSiteEnergyConsumption(currentSite); // J
      runningSites.get(currentSite.getID()).setQepExecutionCost(qepExecutionCost);
    }
    
  }

  private void removeEnergyLevels(HashMap<String, RunTimeSite> runningSites,
      SensorNetworkQueryPlan qep, Integer agendas)
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    Iterator<Node> siteIterator = this.getWsnTopology().getNodes().iterator();
    while(siteIterator.hasNext())
    {
      Site currentSite = (Site) siteIterator.next();
      Double qepExecutionCost = qep.getAgendaIOT().getSiteEnergyConsumption(currentSite); // J
      runningSites.get(currentSite.getID()).removeDefinedCost(qepExecutionCost * agendas);
    }
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
    }
    catch (WhenSchedulerException e)
    {
      throw new SNEECompilerException(e);
    }
    
    boolean success = assessQEPsAgendas(this.currentQEP.getIOT(), newIOT, this.currentQEP.getAgendaIOT(),
                                        newAgendaIOT, newAgenda, false, adapt, failedNodes, routingTree, true, 
                                        this.currentQEP.getDLAF(), this.currentQEP.getID(), this.currentQEP.getCostParameters());
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
