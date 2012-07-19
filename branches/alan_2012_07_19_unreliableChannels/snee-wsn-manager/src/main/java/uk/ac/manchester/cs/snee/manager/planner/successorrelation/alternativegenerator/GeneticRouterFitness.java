package uk.ac.manchester.cs.snee.manager.planner.successorrelation.alternativegenerator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;

import com.rits.cloning.Cloner;

import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.costmodels.avroracosts.AvroraCostParameters;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceWhereSchedular;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.CommunicationTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.sn.router.Router;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenScheduler;
import uk.ac.manchester.cs.snee.manager.common.AutonomicManagerComponent;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.planner.common.Successor;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;
import uk.ac.manchester.cs.snee.sncb.TinyOS_SNCB_Controller;

public class GeneticRouterFitness extends AutonomicManagerComponent
{
  private static final long serialVersionUID = 6457972788990707040L;
  private double lowEnergyTheshold = 0.0;
  private File outputFolder;
  private MetadataManager metamanager;
  private Successor successor;
  private Topology network;
  private ArrayList<String> nodeIds;
  private static int planCounter = 1;
  private boolean usecostModelForPackets;

  public GeneticRouterFitness(Successor successor, File outputFolder, 
                              MetadataManager metamanager, Topology network,
                              ArrayList<String> nodeIds, SourceMetadataAbstract _metadataManager,
                              Boolean usecostModelForPackets)
  throws IOException, SchemaMetadataException, TypeMappingException,
  OptimizationException, CodeGenerationException
  {
    this.outputFolder = outputFolder;
    this._metadata = _metadataManager;
    this.successor = successor;
    this.metamanager = metamanager;
    this.network = network;
    this.nodeIds = nodeIds;
    lowEnergyTheshold = determineEnergyTheshold(successor);
    this.usecostModelForPackets = usecostModelForPackets;
  }
  
  /**
   * start method to determining the threshold of a low energy site.
   * @param qep
   * @return
   * @throws CodeGenerationException 
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws IOException 
   */
  private double determineEnergyTheshold(Successor qep)
  throws IOException, SchemaMetadataException, TypeMappingException, 
  OptimizationException, CodeGenerationException 
  {
    Site worseSite = locateWorseSiteForQEPCost(qep.getCopyOfRunTimeSites(), qep.getQep().getRT());
    //locate cost of reprogramming site
    double reprogrammingCost = locateReprogrammingCost(worseSite, qep);
    double qepCost = qep.getNewRunTimeSites().get(worseSite.getID()).getQepExecutionCost();
    double energyCostToBalanceOutReprogrammingCost = (reprogrammingCost *2) + (qepCost * 10);
    return energyCostToBalanceOutReprogrammingCost;
  }
  
  /**
   * discovers the cost of reprogramming a specific node
   * @param worseSite
   * @return
   * @throws IOException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws OptimizationException
   * @throws CodeGenerationException
   */
  private double locateReprogrammingCost(Site worseSite, Successor successor) 
  throws IOException, SchemaMetadataException, TypeMappingException,
  OptimizationException, CodeGenerationException
  {
    Long fileSize = new Long(0);
    if(this.usecostModelForPackets)
    {
      TinyOS_SNCB_Controller imageGenerator = new TinyOS_SNCB_Controller();
      File adaptFolder = new File(this.outputFolder.toString() + sep + "worseSiteTemp");
      //create folder for this adaptation 
      adaptFolder.mkdir();
      imageGenerator.generateNesCCode(successor.getQep(), adaptFolder.toString() + sep, metamanager);
      ArrayList<String> sites = new ArrayList<String>();
      sites.add(worseSite.getID());
      imageGenerator.compileReducedNesCCode(adaptFolder.toString()+ sep, sites);
      File moteQEP = new File(adaptFolder.toString() + sep + "avrora_micaz_t2" + sep + "mote" + worseSite.getID() + ".elf");
      if(moteQEP.exists())
        fileSize = moteQEP.length();
      else
      {
        fileSize = (long) 0; //needs to be changed back to error. results in issue with joins where join is on sink for code generator
        //throw new IOException("cant find image");
      }
    }
    else
    {
      
    }
    //locate packets required to be recieved.
    CostParameters parameters = metamanager.getCostParameters();
    int packetSize = parameters.getDeliverPayloadSize();
    Long packets = fileSize / packetSize;
    //calcualte radio cost
    double radioRXAmp = AvroraCostParameters.getRadioReceiveAmpere();
    double duration = AgendaIOT.bmsToMs((packets * (long) Math.ceil(parameters.getSendPacket() * packets)) +
                      CommunicationTask.getTimeCostOverhead(parameters)) / new Double(1000);
    double recievecost =  duration * radioRXAmp * AvroraCostParameters.VOLTAGE;
    //calculate flash cost
    double costPerByteWritten = AvroraCostParameters.VOLTAGE * AvroraCostParameters.FlashWRITECYCLES * 
    AvroraCostParameters.CYCLETIME * AvroraCostParameters.FlashWRITEAMPERE;
    double bytes = packets * parameters.getDeliverPayloadSize();
    double flashcost =  bytes * costPerByteWritten; 
    return recievecost + flashcost;
  }
  
  private Site locateWorseSiteForQEPCost(HashMap<String, RunTimeSite> runTimeSites, RT succesorRT)
  {
    double qepCost = Double.MIN_VALUE;
    Site worseSite = null;
    Iterator<String> keyIterator = runTimeSites.keySet().iterator();
    while(keyIterator.hasNext())
    {
      String key = keyIterator.next();
      RunTimeSite site = runTimeSites.get(key);
      if(site.getQepExecutionCost() >= qepCost)
      {
        qepCost = site.getQepExecutionCost();
        worseSite = succesorRT.getSite(key);
      }
    }
    return worseSite;
  }

  
  public Phenome determineFitness(Genome newPop, int successorAgendaCount)
  {
    Cloner cloner = new Cloner();
    cloner.dontClone(Logger.class);
    Phenome phenome = new Phenome(newPop, successorAgendaCount);
    Topology currentTopology = cloner.deepClone(network);
    
    int counter = 0;
    Iterator<Boolean> geneIterator = newPop.geneIterator();
    while(geneIterator.hasNext())
    {
      Boolean geneValue = geneIterator.next();
      if(!geneValue)
      {
        currentTopology.removeNode(this.nodeIds.get(counter));
        currentTopology.removeAssociatedEdges(this.nodeIds.get(counter));
      }
      counter ++;
    }
    try
    {
     // new TopologyUtils(currentTopology).exportAsDOTFile(this.outputFolder.toString() + sep + "currentTop", false);
      Router router = new Router();
      RT rt = router.doRouting(this.successor.getQep().getIOT().getPAF(), "", currentTopology, _metadata);
      phenome.setRt(rt);
      //locate any site swith low energy
      ArrayList<String> lowEnergySites = locateLowEnergySites(phenome);
      //search though low energy sites, if any exist in the topology, then fitness =0
      Iterator<String> lowEnergySiteIterator = lowEnergySites.iterator();
      while(lowEnergySiteIterator.hasNext())
      {
        String siteID = lowEnergySiteIterator.next();
        if(rt.getSite(siteID) != null && !rt.getRoot().getID().equals(siteID))
        {
          return new Phenome(0);
        }
      }
      SensorNetworkQueryPlan plan = convertRTToQEP(rt);
      if(plan == null)
        return new Phenome(0);
      else
      {
        phenome.getSuccessor().setQep(plan);
        phenome.getSuccessor().updateSitesRunningCosts();
        phenome.setFitness(1);
        return phenome;
      }
    }
    catch(Exception e)
    {
    //  e.printStackTrace();
      return new Phenome(0);
    }
  }
  
  private ArrayList<String> locateLowEnergySites(Phenome phenome)
  {
    HashMap<String, RunTimeSite> runtimeSites = removeQEPRunningCost(phenome.getGenomeTime());
    phenome.getSuccessor().setNewRunTimeSites(runtimeSites);
    ArrayList<String> lowEnergySites = new ArrayList<String>();
    Iterator<String> runtimeSiteKeyIterator = runtimeSites.keySet().iterator();
    while(runtimeSiteKeyIterator.hasNext())
    {
      String key = runtimeSiteKeyIterator.next();
      RunTimeSite site = runtimeSites.get(key);
      if(site.getCurrentEnergy() <= this.lowEnergyTheshold)
        lowEnergySites.add(key);
    } 
    return lowEnergySites;
  }
  
  private SensorNetworkQueryPlan convertRTToQEP(RT rt) 
  {
    try
    {
      //System.out.println("storing alternative " + planCounter);
      ///File planOutputFolder =  
      //  new File(this.outputFolder.toString() + sep + "plan" + planCounter);
      //planOutputFolder.mkdir();
      //do where scheduling
      InstanceWhereSchedular whereScheduling = 
        new InstanceWhereSchedular(this.successor.getQep().getDAF().getPAF(), rt, 
                                   this.successor.getQep().getCostParameters(), false);
      IOT iot = whereScheduling.getIOT();
      DAF daf = whereScheduling.getDAF();
      
      //do when scheduling
      boolean useNetworkController = 
        SNEEProperties.getBoolSetting(SNEEPropertyNames.SNCB_INCLUDE_COMMAND_SERVER);
      boolean allowDiscontinuousSensing = 
        SNEEProperties.getBoolSetting(SNEEPropertyNames.ALLOW_DISCONTINUOUS_SENSING);
      WhenScheduler whenScheduling = 
        new WhenScheduler(allowDiscontinuousSensing, this.successor.getQep().getCostParameters(), 
                          useNetworkController);
      Agenda agenda = whenScheduling.doWhenScheduling(daf, this.successor.getQep().getQos(),
                                                      this.successor.getQep().getQueryName());
      AgendaIOT agendaIOT = 
        whenScheduling.doWhenScheduling(iot, this.successor.getQep().getQos(), 
                                        this.successor.getQep().getQueryName(), 
                                        this.successor.getQep().getCostParameters());
      //new AgendaIOTUtils(agendaIOT, iot, true).generateImage(planOutputFolder.toString());
      
      SensorNetworkQueryPlan newPlan = new SensorNetworkQueryPlan(this.successor.getQep().getDLAF(), rt,  
                                     iot, agendaIOT, agenda, this.successor.getQep().getQueryName() + 
                                     ":ALT" + planCounter, this.successor.getQoS());
      planCounter++;
      return newPlan;
    }
    catch(Exception e)
    {
      System.out.println("genotype failed to pass though the snee stack. Will produce a fitness of 0");
      e.printStackTrace();
      return null;
    }
  }
  
  private HashMap<String, RunTimeSite> removeQEPRunningCost(int randomLifetime)
  {
    HashMap<String, RunTimeSite> runtimeSites = successor.getCopyOfRunTimeSites();
    for(int cycle = 0; cycle < randomLifetime; cycle++)
    {
      Iterator<String> keyIterator = runtimeSites.keySet().iterator();
      while(keyIterator.hasNext())
      {
        String key = keyIterator.next();
        RunTimeSite site = runtimeSites.get(key);
        site.removeQEPExecutionCost();
      }
    }
    return runtimeSites;
  }
  
  
  
  
}
