package uk.ac.manchester.cs.snee.manager.planner.successorrelation.alternativegenerator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.logging.Logger;

import com.rits.cloning.Cloner;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.graph.Tree;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.WhenSchedulerException;
import uk.ac.manchester.cs.snee.compiler.costmodels.avroracosts.AvroraCostParameters;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOTUtils;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceWhereSchedular;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.CommunicationTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenScheduler;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.AutonomicManagerComponent;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.failednode.alternativerouter.CandiateRouter;
import uk.ac.manchester.cs.snee.manager.planner.common.Successor;
import uk.ac.manchester.cs.snee.manager.planner.successorrelation.tabu.TABUList;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;
import uk.ac.manchester.cs.snee.sncb.TinyOS_SNCB_Controller;

public class AlternativeGenerator extends AutonomicManagerComponent
{

  private static final long serialVersionUID = -2274933349883496847L;
  private Successor successor;
  private File outputFolder;
  private SourceMetadataAbstract _metadata;
  private String sep = System.getProperty("file.separator");
  private int maxNeighbourHoodGeneration = 5;
  private MetadataManager metamanager = null;
  /**
   * constructor
   * @param successor
   * @param topology 
   * @param _metadata 
   * @param _metaManager 
   */
  public AlternativeGenerator(File outputFolder, SourceMetadataAbstract _metadata, 
                              AutonomicManagerImpl manager, MetadataManager _metaManager)
  {
	  this.manager = manager;
	  this.outputFolder = outputFolder;
	  this._metadata = _metadata;
	  this.metamanager = _metaManager;
	  
  }
  
  /**
   * generates a set of alternative qeps by using different decisions from the snee stack. 
   * @param aspirationPlusBounds 
   * @param tabuList 
   * @param position 
   * @return
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   * @throws OptimizationException 
   * @throws SchemaMetadataException 
   * @throws SNEEException 
   * @throws WhenSchedulerException 
   * @throws TypeMappingException 
   * @throws CodeGenerationException 
   * @throws IOException 
   */
  public ArrayList<Successor> generateAlternatives(Successor successor, 
                                                   int aspirationPlusBounds,
                                                   TABUList tabuList, int position) 
  throws NumberFormatException, SNEEConfigurationException, 
  SNEEException, SchemaMetadataException, OptimizationException, 
  WhenSchedulerException, TypeMappingException, IOException, CodeGenerationException
  {
    this.successor = successor;
    int attempts = 0;
    ArrayList<Successor> successors = new ArrayList<Successor>();
    while(successors.size() < aspirationPlusBounds && attempts < maxNeighbourHoodGeneration)
    {
      //choose time to switch and remvoe cost of running for tiem period
      int randomLifetime = randomtimeGenerator();
      HashMap<String, RunTimeSite> runtimeSites = removeQEPRunningCost(randomLifetime);
      //remove dead nodes
      Topology workingTopology = reduceTopology(runtimeSites);
      //run hueristic router and then genetic router seeded with the huristic router.
      ArrayList<RT> candidateRoutes = HuristicRouter(this.successor.getQep().getDAF().getPAF(), workingTopology);
      ArrayList<Tree> geneticRoutes = GeneticRouter(candidateRoutes, workingTopology);
      //add routes together
      Iterator<RT> heuristicTrees = candidateRoutes.iterator();
      while(heuristicTrees.hasNext())
      {
        RT heursticTree = heuristicTrees.next();
        geneticRoutes.add(heursticTree.getSiteTree());
      }
      //remove duplicates
      geneticRoutes = CandiateRouter.removeDuplicates(geneticRoutes);
      //turn trees into rts
      Iterator<Tree> finalTreeSetIterator = geneticRoutes.iterator();
      ArrayList<RT> finalRTs = new ArrayList<RT>();
      int routingTreeID = 1;
      while(finalTreeSetIterator.hasNext())
      {
        Tree tree = finalTreeSetIterator.next();
        finalRTs.add(new RT(successor.getQep().getIOT().getPAF(), "Alt" + routingTreeID, tree, workingTopology));
        routingTreeID++;
      }
      
      //turn rts into qeps and then into successors
      Iterator<SensorNetworkQueryPlan> qepIterator = 
        new ArrayList<SensorNetworkQueryPlan>(convertRTToQEP(finalRTs)).iterator();
      while(qepIterator.hasNext())
      {
        successors.add(new Successor(qepIterator.next(), randomLifetime, successor.getCopyOfRunTimeSites() ,successor.getAgendaCount()));
      }
      
      //remove all entire tabued plans
      ArrayList<Successor> toremove = new ArrayList<Successor>();
      Iterator<Successor> planIterator = successors.iterator();
      while(planIterator.hasNext())
      {
        Successor plan = planIterator.next();
        if(tabuList.isEntirelyTABU(plan.getQep(), position))
        {
          toremove.add(plan);
        }
      }
      planIterator= toremove.iterator();
      while(planIterator.hasNext())
      {
        Successor plan = planIterator.next();
        successors.remove(plan);
      }
    }
    return successors;
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

  private int randomtimeGenerator()
  {
    int maxLifetimeInAgendas = successor.getBasicLifetimeInAgendas();
    Random random = new Random();
    return random.nextInt(maxLifetimeInAgendas);
  }

  private ArrayList<SensorNetworkQueryPlan> convertRTToQEP(
      ArrayList<RT> finalRTs) 
      throws SNEEException, SchemaMetadataException, OptimizationException, 
      SNEEConfigurationException, WhenSchedulerException, TypeMappingException
  {
    ArrayList<SensorNetworkQueryPlan> qeps = new ArrayList<SensorNetworkQueryPlan>();
    Iterator<RT> routeIterator = finalRTs.iterator();
    int routeCounter = 1;
    System.out.println("storing alternatives");
    while(routeIterator.hasNext())
    {
      System.out.println("storing alternative " + routeCounter);
      File planOutputFolder =  
        new File(this.outputFolder.toString() + sep + "plan" + routeCounter);
      planOutputFolder.mkdir();
      RT routingTree = routeIterator.next();
      //do where scheduling
      InstanceWhereSchedular whereScheduling = 
        new InstanceWhereSchedular(this.successor.getQep().getDAF().getPAF(), routingTree, 
                                   this.successor.getQep().getCostParameters(),
                                    planOutputFolder.toString());
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
      Agenda agenda = whenScheduling.doWhenScheduling(daf, this.successor.getQep().getQos(), this.successor.getQep().getQueryName());
      AgendaIOT agendaIOT = 
        whenScheduling.doWhenScheduling(iot, this.successor.getQep().getQos(), this.successor.getQep().getQueryName(), 
                                        this.successor.getQep().getCostParameters());
      new AgendaIOTUtils(agendaIOT, iot, true).generateImage(planOutputFolder.toString());
      qeps.add(
          new SensorNetworkQueryPlan(this.successor.getQep().getDLAF(), routingTree, daf, 
                                     iot, agendaIOT, agenda, this.successor.getQep().getQueryName() + 
                                     ":ALT" + routeCounter ));
      routeCounter++;
    }
    System.out.println("finished storing alternatives");
    return qeps;
  }

  private ArrayList<RT> HuristicRouter(PAF paf, Topology workingTopology) 
  throws NumberFormatException, SNEEConfigurationException
  {
    CandiateRouter metaRouter = 
      new CandiateRouter(workingTopology, outputFolder, this.successor.getQep().getDAF().getPAF(), 
                         this._metadata);
    return metaRouter.generateAlternativeRoutingTrees(this.successor.getQep().getQueryName());
  }

  private Topology reduceTopology(HashMap<String, RunTimeSite> runtimeSites) 
  throws IOException, SchemaMetadataException, TypeMappingException, 
  OptimizationException, CodeGenerationException
  {
    Cloner cloner = new Cloner();
    cloner.dontClone(Logger.class);
    Topology copyOfNetwork = cloner.deepClone(manager.getWsnTopology());
    //locate most costly site
    Site worseSite = locateWorseSiteForQEPCost(runtimeSites);
    //locate cost of reprogramming site
    double reprogrammingCost = locateReprogrammingCost(worseSite);
    double qepCost = runtimeSites.get(worseSite.getID()).getQepExecutionCost();
    double energyCostToBalanceOutReprogrammingCost = (reprogrammingCost *2) + (qepCost * 10);
    //locate any sites which do not meet this level of energy
    Iterator<String> keyIterator = runtimeSites.keySet().iterator();
    while(keyIterator.hasNext())
    {
      String key = keyIterator.next();
      RunTimeSite site = runtimeSites.get(key);
      if(!this.successor.getQep().getRT().getRoot().getID().equals(key))
      {
        if(site.getCurrentEnergy() <= energyCostToBalanceOutReprogrammingCost)
        {
          copyOfNetwork.removeNode(key);
        }
      }
    }
    return copyOfNetwork;
  }

  private double locateReprogrammingCost(Site worseSite) 
  throws IOException, SchemaMetadataException, TypeMappingException,
  OptimizationException, CodeGenerationException
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
    Long fileSize = new Long(0);
    if(moteQEP.exists())
      fileSize = moteQEP.length();
    else
    {
      fileSize = (long) 0; //needs to be changed back to error. results in issue with joins where join is on sink for code generator
      //throw new IOException("cant find image");
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

  private Site locateWorseSiteForQEPCost(HashMap<String, RunTimeSite> runtimeSites)
  {
    double qepCost = Double.MIN_VALUE;
    Site worseSite = null;
    Iterator<String> keyIterator = runtimeSites.keySet().iterator();
    while(keyIterator.hasNext())
    {
      String key = keyIterator.next();
      RunTimeSite site = runtimeSites.get(key);
      if(site.getQepExecutionCost() >= qepCost)
      {
        qepCost = site.getQepExecutionCost();
        worseSite = this.successor.getQep().getRT().getSite(key);
      }
    }
    return worseSite;
  }

  private ArrayList<Tree> GeneticRouter(ArrayList<RT> candidateRoutes,
                                        Topology workingTopology)
  {
    GeneticRouter geneticRouter = 
      new GeneticRouter(_metadata, workingTopology, this.successor.getQep().getIOT().getPAF(), 
                        outputFolder);
    return geneticRouter.generateAlternativeRoutes(candidateRoutes);  
  }
} 
