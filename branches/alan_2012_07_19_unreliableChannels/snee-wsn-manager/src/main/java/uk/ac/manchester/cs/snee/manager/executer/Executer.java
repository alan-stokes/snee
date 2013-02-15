package uk.ac.manchester.cs.snee.manager.executer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.costmodels.HashMapList;
import uk.ac.manchester.cs.snee.compiler.costmodels.cardinalitymodel.CardinalityEstimatedCostModel;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.AutonomicManagerComponent;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.LogicalOverlayStrategy;
import uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.channel.ChannelModel;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.LogicalOverlayNetworkHierarchy;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.RobustSensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;

public class Executer extends AutonomicManagerComponent
{

  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -5028022484881855975L;
  private HashMapList<Executer.type, Integer> tuplesReturnedFromEachTypeOfQEP = 
    new HashMapList<Executer.type, Integer>();
  private File executerOutputFolder = null;
  private enum type{MAX, RQEP, QEP};
  private HashMap<String, RunTimeSite> runningSites;
  private Topology network;
  
  public Executer(AutonomicManagerImpl autonomicManager)
  {
    manager = autonomicManager;
  }

  public void adapt(Adaptation finalChoice)
  {
    manager.setCurrentQEP(finalChoice.getNewQep());
    manager.updateStrategies(finalChoice);
    runningSites = manager.getCopyOfRunningSites();
    this.network = manager.getWsnTopology();
  }

  /**
   * writes all data stored currently in the hashmaplist of results generated by calls to 
   * simulateRunOfRQEP and simulateRunOfQEP
   * @param distanceFactorFolder 
   * @throws IOException
   */
  public void writeResultsToFile(File distanceFactorFolder)
  throws IOException
  {
    //make writer
    File outFile = new File(distanceFactorFolder.toString() + sep + "tupleOutput");
    BufferedWriter out = new BufferedWriter(new FileWriter(outFile));
    //iterate over the 3 results.
    ArrayList<Integer> max = tuplesReturnedFromEachTypeOfQEP.get(type.MAX);
    ArrayList<Integer> rQEP = tuplesReturnedFromEachTypeOfQEP.get(type.RQEP);
    ArrayList<Integer> QEP = tuplesReturnedFromEachTypeOfQEP.get(type.QEP);
    /*assumption is that all lists have the same number of elements (should be true, due 
    to them being ran the same number of times.
    */
    Iterator<Integer> maxIterator = max.iterator();
    Iterator<Integer> rQEPIterator = rQEP.iterator();
    Iterator<Integer> QEPIterator = QEP.iterator();
    int counter = 1;
    
    while(maxIterator.hasNext())
    {
      Integer maxValue = maxIterator.next();
      Integer rQEPValue = rQEPIterator.next();
      Integer QEPValue = QEPIterator.next();
      out.write(counter + " " + maxValue + " " + QEPValue + " " + rQEPValue + "\n");
      counter++;
    }
    out.flush();
    out.close();
  }
  
  
  /**
   * simulates a run of a number of agenda cycles governed by the 
   * SNEEproperties.wsn_manager.unreliable.channels.simulationLength parameter or 
   * the default value set in SNEEProperties.getSetting().
   * for a Robust Sensor network query plan
   * @param rQEP
   * @param channelModel
   * @throws NumberFormatException
   * @throws SNEEConfigurationException
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  public ArrayList<RunTimeSite> simulateRunOfRQEP(RobustSensorNetworkQueryPlan rQEP, 
                                ChannelModel channelModel)
  throws NumberFormatException, SNEEConfigurationException,
  OptimizationException, SchemaMetadataException, TypeMappingException
  {
    Integer numberOfIterations = Integer.parseInt(SNEEProperties.getSetting(
        SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS_SIMULATION_ITERATIONS));
    CardinalityEstimatedCostModel model = 
      new CardinalityEstimatedCostModel(rQEP.getUnreliableAgenda(), rQEP.getRT(), rQEP.getIOT());
    model.runModel();
    // fixed iterations
    numberOfIterations = 50;
    HashMapList<String, RunTimeSite> sitesEnergyValues = new HashMapList<String, RunTimeSite>();
    //cycle though iterations
    for(int iteration = 0; iteration < numberOfIterations; iteration ++)
    {
      HashMapList<String, RunTimeSite> iterationSitesEnergyValues = 
        channelModel.runModel(new ArrayList<String>(), rQEP.getIOT());
      Iterator<String> keyIterator = iterationSitesEnergyValues.keySet().iterator();
      while(keyIterator.hasNext())
      {
        String key = keyIterator.next();
        sitesEnergyValues.addAll(key, iterationSitesEnergyValues.get(key));
      }
      Integer tuplesReturned = determineTupleDeliveryRate(channelModel, rQEP);
      Integer maxTuplesReturnable = (int) model.returnAgendaExecutionResult();
      tuplesReturnedFromEachTypeOfQEP.addWithDuplicates(type.RQEP, tuplesReturned);
      tuplesReturnedFromEachTypeOfQEP.addWithDuplicates(type.MAX, maxTuplesReturnable);
      channelModel.clearModel();
      rQEP.getLogicalOverlayNetwork().removeClonedData();
    }
    
    ArrayList<RunTimeSite> siteAverageEnergyDepletion = new ArrayList<RunTimeSite>();
    Iterator<String> keyIterator = sitesEnergyValues.keySet().iterator();
    while(keyIterator.hasNext())
    {
      String key = keyIterator.next();
      Iterator<RunTimeSite> siteQEPCostIterator = sitesEnergyValues.get(key).iterator();
      double qepCostTotaller = 0.0;
      while(siteQEPCostIterator.hasNext())
      {
        RunTimeSite siteValue = siteQEPCostIterator.next();
        qepCostTotaller+= siteValue.getQepExecutionCost();
      }
      qepCostTotaller = qepCostTotaller / numberOfIterations;
      RunTimeSite average = new RunTimeSite(0.0, key, qepCostTotaller);
      siteAverageEnergyDepletion.add(average);
    }
    
    return siteAverageEnergyDepletion;
  }
  
  /**
   * simulates a run of a number of agenda cycles governed by the 
   * SNEEproperties.wsn_manager.unreliable.channels.simulationLength parameter or 
   * the default value set in SNEEProperties.getSetting().
   * for a Sensor network query plan
   * @param rQEP
   * @param channelModel
   * @return 
   * @throws NumberFormatException
   * @throws SNEEConfigurationException
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  public ArrayList<RunTimeSite> simulateRunOfQEP(SensorNetworkQueryPlan QEP, 
                                ChannelModel channelModel)
  throws NumberFormatException, SNEEConfigurationException,
  OptimizationException, SchemaMetadataException, TypeMappingException
  {
    Integer numberOfIterations = Integer.parseInt(SNEEProperties.getSetting(
        SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS_SIMULATION_ITERATIONS));
    numberOfIterations = 50;
    channelModel.setPacketModel(false); 
    HashMapList<String, RunTimeSite> sitesEnergyValues = new HashMapList<String, RunTimeSite>();
    for(int iteration = 0; iteration < numberOfIterations; iteration ++)
    {
      HashMapList<String, RunTimeSite> iterationSitesEnergyValues = 
        channelModel.runModel(new ArrayList<String>(), QEP.getIOT());
      Iterator<String> keyIterator = iterationSitesEnergyValues.keySet().iterator();
      while(keyIterator.hasNext())
      {
        String key = keyIterator.next();
        sitesEnergyValues.addAll(key, iterationSitesEnergyValues.get(key));
      }
      Integer tuplesReturned = determineTupleDeliveryRate(channelModel, QEP);
      tuplesReturnedFromEachTypeOfQEP.addWithDuplicates(type.QEP, tuplesReturned);
      channelModel.clearModel();
    }
    
    ArrayList<RunTimeSite> siteAverageEnergyDepletion = new ArrayList<RunTimeSite>();
    Iterator<String> keyIterator = sitesEnergyValues.keySet().iterator();
    while(keyIterator.hasNext())
    {
      String key = keyIterator.next();
      Iterator<RunTimeSite> siteQEPCostIterator = sitesEnergyValues.get(key).iterator();
      double qepCostTotaller = 0.0;
      while(siteQEPCostIterator.hasNext())
      {
        RunTimeSite siteValue = siteQEPCostIterator.next();
        qepCostTotaller+= siteValue.getQepExecutionCost();
      }
      qepCostTotaller = qepCostTotaller / numberOfIterations;
      RunTimeSite average = new RunTimeSite(0.0, key, qepCostTotaller);
      siteAverageEnergyDepletion.add(average);
    }
    return siteAverageEnergyDepletion;
  }
  
  

  /**
   * determines the number of tuples returned by the deliery operator from a cycle of the agenda.
   * used on a Robust Sensor network query plan
   */
  private int determineTupleDeliveryRate(ChannelModel channelModel,
                                          RobustSensorNetworkQueryPlan rQEP)
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    Site rootSite = rQEP.getRT().getRoot();
    Integer packets = channelModel.packetsToBeSentByDeliveryTask(rootSite, rQEP.getIOT());
    int tuples = channelModel.packetToTupleConversion(packets, rootSite);
    return tuples;
  }
  
  /**
   * determines the number of tuples returned by the deliery operator from a cycle of the agenda.
   * used on a Robust Sensor network query plan
   */
  private int determineTupleDeliveryRate(ChannelModel channelModel,
                                          SensorNetworkQueryPlan QEP)
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    Site rootSite = QEP.getRT().getRoot();
    Integer packets = channelModel.packetsToBeSentByDeliveryTask(rootSite, QEP.getIOT());
    int tuples = channelModel.packetToTupleConversion(packets, rootSite);
    return tuples;
  }
 
  public void simulateRunOfQEPs(RobustSensorNetworkQueryPlan rQEP,
                                SensorNetworkQueryPlan qep)
  throws NumberFormatException, SNEEConfigurationException, OptimizationException,
  SchemaMetadataException, TypeMappingException, IOException
  {
    for(double distanceFactor = 0.2; distanceFactor <= 1; distanceFactor +=0.2)
    {
      SNEEProperties.setSetting("distanceFactor", new Double(distanceFactor).toString());
      File distanceFactorFolder = 
        new File(this.executerOutputFolder.toString() + sep + distanceFactor);
      distanceFactorFolder.mkdir();
      File robustFolder = new File(distanceFactorFolder.toString() + sep + "robust");
      robustFolder.mkdir();
      ChannelModel channelModel = 
        new ChannelModel(rQEP.getLogicalOverlayNetwork(), rQEP.getUnreliableAgenda(), 
                         manager.getWsnTopology().getMaxNodeID(), manager.getWsnTopology(),
                         manager.getCostsParamters(), robustFolder);
      simulateRunOfRQEP(rQEP, channelModel);
      LogicalOverlayNetworkHierarchy skelOverlay = 
        new LogicalOverlayNetworkHierarchy(rQEP.getLogicalOverlayNetwork(),
                                           qep,  manager.getWsnTopology());
      File staticFolder = new File(distanceFactorFolder.toString() + sep + "static");
      staticFolder.mkdir();
      channelModel = 
        new ChannelModel(skelOverlay, qep.getAgendaIOT(), 
                         manager.getWsnTopology().getMaxNodeID(), manager.getWsnTopology(),
                         manager.getCostsParamters(), staticFolder);
      simulateRunOfQEP(qep, channelModel);
      writeResultsToFile(distanceFactorFolder);
      cleardataStores();
    }
    
    //one last time for 0.1 format.
    double distanceFactor = 0.1;
    SNEEProperties.setSetting("distanceFactor", new Double(distanceFactor).toString());
    File distanceFactorFolder = 
      new File(this.executerOutputFolder.toString() + sep + distanceFactor);
    distanceFactorFolder.mkdir();
    File robustFolder = new File(distanceFactorFolder.toString() + sep + "robust");
    robustFolder.mkdir();
    ChannelModel channelModel = 
      new ChannelModel(rQEP.getLogicalOverlayNetwork(), rQEP.getUnreliableAgenda(), 
                       manager.getWsnTopology().getMaxNodeID(), manager.getWsnTopology(),
                       manager.getCostsParamters(), robustFolder);
    simulateRunOfRQEP(rQEP, channelModel);
    LogicalOverlayNetworkHierarchy skelOverlay = 
      new LogicalOverlayNetworkHierarchy(rQEP.getLogicalOverlayNetwork(),
                                         qep,  manager.getWsnTopology());
    File staticFolder = new File(distanceFactorFolder.toString() + sep + "static");
    staticFolder.mkdir();
    channelModel = 
      new ChannelModel(skelOverlay, qep.getAgendaIOT(), 
                       manager.getWsnTopology().getMaxNodeID(), manager.getWsnTopology(),
                       manager.getCostsParamters(), staticFolder);
    simulateRunOfQEP(qep, channelModel);
    writeResultsToFile(distanceFactorFolder);
    cleardataStores();
    
  }
  

  private void cleardataStores()
  {
    this.tuplesReturnedFromEachTypeOfQEP = new HashMapList<Executer.type, Integer>();
  }

  public void setUpFolders(File outputFolder)
  {
    File mainOutputFolderOfManager = manager.getOutputFolder();
    executerOutputFolder = new File(mainOutputFolderOfManager.toString() + sep + "executer");
    if(!executerOutputFolder.mkdir())
      System.out.println("couldnt make file");
  }

  public void simulateRunOfQEPs(RobustSensorNetworkQueryPlan rQEP, 
                                SensorNetworkQueryPlan qep, Long seed, Double distanceFactor)
  throws NumberFormatException, SNEEConfigurationException, OptimizationException,
  SchemaMetadataException, TypeMappingException, IOException
  {
      SNEEProperties.setSetting("distanceFactor", new Double(distanceFactor).toString());
      File distanceFactorFolder = 
        new File(this.executerOutputFolder.toString() + sep + distanceFactor);
      distanceFactorFolder.mkdir();
      File robustFolder = new File(distanceFactorFolder.toString() + sep + "robust");
      robustFolder.mkdir();
      ChannelModel channelModel = 
        new ChannelModel(rQEP.getLogicalOverlayNetwork(), rQEP.getUnreliableAgenda(), 
                         manager.getWsnTopology().getMaxNodeID(), manager.getWsnTopology(),
                         manager.getCostsParamters(), robustFolder, seed);
      simulateRunOfRQEP(rQEP, channelModel);
      LogicalOverlayNetworkHierarchy skelOverlay = 
        new LogicalOverlayNetworkHierarchy(rQEP.getLogicalOverlayNetwork(),
                                           qep,  manager.getWsnTopology());
      File staticFolder = new File(distanceFactorFolder.toString() + sep + "static");
      staticFolder.mkdir();
      channelModel = 
        new ChannelModel(skelOverlay, qep.getAgendaIOT(), 
                         manager.getWsnTopology().getMaxNodeID(), manager.getWsnTopology(),
                         manager.getCostsParamters(), staticFolder, seed);
      simulateRunOfQEP(qep, channelModel);
      writeResultsToFile(distanceFactorFolder);
      cleardataStores();
  }

  public void calculateLifetimeDifferenceFromDeployments(RobustSensorNetworkQueryPlan rQEP, 
                                                         SensorNetworkQueryPlan qep, Long seed,
                                                         Double distanceConverter)
  throws NumberFormatException, SNEEConfigurationException, OptimizationException,
  SchemaMetadataException, TypeMappingException, IOException, CodeGenerationException
  {
    SNEEProperties.setSetting("distanceFactor", new Double(distanceConverter).toString());
    File distanceFactorFolder = 
      new File(this.executerOutputFolder.toString() + sep + distanceConverter);
    distanceFactorFolder.mkdir();
    File robustFolder = new File(distanceFactorFolder.toString() + sep + "lifetimeEstimate");
    robustFolder.mkdir();
    
    double overallRQEPShortestLifetime = calculateOverallRQEPShortestLifetime(rQEP, robustFolder, seed);
    this.runningSites = manager.getCopyOfRunningSites();
    double overallQEPShortestLifetime = calculateOverallQEPShortestLifetime(qep, robustFolder, seed, rQEP);
    
    String queryid = manager.getQueryID();
    DecimalFormat format = new DecimalFormat("#.##");
    BufferedWriter out = new BufferedWriter(new FileWriter(new File(robustFolder.toString() + sep + "lifetimes")));
    out.write(queryid + " " + format.format(overallQEPShortestLifetime) + " " + format.format(overallRQEPShortestLifetime));
    out.flush();
    out.close();
  }

  
  /**
   * calculates the lifetime of the basic overlay network
   * @param QEP
   * @param robustFolder
   * @param seed
   * @param rQEP
   * @return
   * @throws OptimizationException 
   * @throws CodeGenerationException 
   * @throws SNEEConfigurationException 
   * @throws IOException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   */
  private double calculateOverallQEPShortestLifetime(SensorNetworkQueryPlan QEP, 
                                                     File robustFolder, Long seed,
                                                     RobustSensorNetworkQueryPlan rQEP) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException,
  IOException, SNEEConfigurationException, CodeGenerationException
  {
    boolean alive = true;
    ArrayList<String> globalFailedNodes = new ArrayList<String>();
    double overallQEPShortestLifetime = 0;
    LogicalOverlayStrategy logicalOverlayGenerator = new LogicalOverlayStrategy(this.manager, this._metadata,
                                                                                this.manager.get_metadataManager());
    logicalOverlayGenerator.initilise(QEP, 1);
    
    while(alive)
    {
      LogicalOverlayNetworkHierarchy skelOverlay = 
        new LogicalOverlayNetworkHierarchy(logicalOverlayGenerator.getLogicalOverlay(),
                                           qep,  manager.getWsnTopology());
      ChannelModel channelModel = 
        new ChannelModel(skelOverlay, qep.getAgendaIOT(), 
                         manager.getWsnTopology().getMaxNodeID(), manager.getWsnTopology(),
                         manager.getCostsParamters(), robustFolder, seed);
      ArrayList<RunTimeSite> qepCostAveragesQeps = simulateRunOfQEP(QEP, channelModel);
      Iterator<RunTimeSite> newQEPCostIterator = qepCostAveragesQeps.iterator();
      while(newQEPCostIterator.hasNext())
      {
        RunTimeSite newQEPCostContainer = newQEPCostIterator.next();
        RunTimeSite oldQEPCostContainer = this.runningSites.get(newQEPCostContainer.toString());
        oldQEPCostContainer.setQepExecutionCost(newQEPCostContainer.getQepExecutionCost());
      }
      double agendaLength = Agenda.bmsToMs(rQEP.getAgendaIOT().getLength_bms(false))/new Double(1000); // ms to s
      
      //locate weakest node
      Iterator<Node> siteIter = this.network.siteIterator();
      String failedSite = null;
      double shortestLifetime = Double.MAX_VALUE; 
      while (siteIter.hasNext()) 
      {
        Node site = siteIter.next();
        RunTimeSite rSite = runningSites.get(site.getID());
        double currentEnergySupply = rSite.getCurrentEnergy() - rSite.getCurrentAdaptationEnergyCost();
        double siteLifetime = (currentEnergySupply / runningSites.get(site.getID()).getQepExecutionCost());
        boolean useAcquires = SNEEProperties.getBoolSetting(SNEEPropertyNames.WSN_MANAGER_K_RESILENCE_SENSE);
        //uncomment out sections to not take the root site into account
        if (!site.getID().equals(rQEP.getIOT().getRT().getRoot().getID()) &&
           ((useAcquires) ||  (!useAcquires && !((Site) site).isSource())) &&
           !globalFailedNodes.contains(site.getID())) 
        { 
          if(shortestLifetime > siteLifetime)
          {
            if(!((Site) site).isDeadInSimulation())
            {
              shortestLifetime = siteLifetime;
              failedSite = site.getID();
            }
          }
        }
      }
      
      //update runtimeSites energy levels
      updateSitesEnergyLevels(shortestLifetime, globalFailedNodes);
      overallQEPShortestLifetime += (shortestLifetime * agendaLength);
      globalFailedNodes.add(failedSite);
      
      
      if(logicalOverlayGenerator.canAdapt(failedSite, logicalOverlayGenerator.getLogicalOverlay()))
      {
        ArrayList<String> failedNodeIDs = new ArrayList<String>();
        failedNodeIDs.add(failedSite);
        System.out.println("node " + failedSite);
        List<Adaptation> result = logicalOverlayGenerator.adapt(failedNodeIDs, logicalOverlayGenerator.getLogicalOverlay());
        if(result.size() == 0)
          alive = false;
        QEP = result.get(0).getNewQep();
        shortestLifetime =  Double.MAX_VALUE;
      }
      else
      {
        alive = false;
      }
    }
    return overallQEPShortestLifetime;
  }

  /**
   * calculates the lifetime of the robust qEP
   * @param rQEP
   * @param robustFolder
   * @param seed
   * @return
   * @throws SNEEConfigurationException 
   * @throws IOException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws NumberFormatException 
   */
  private double calculateOverallRQEPShortestLifetime(RobustSensorNetworkQueryPlan rQEP, File robustFolder,
                                                      Long seed)
  throws SNEEConfigurationException, IOException, NumberFormatException,
  OptimizationException, SchemaMetadataException, TypeMappingException
  {
    boolean alive = true;
    ArrayList<String> globalFailedNodes = new ArrayList<String>();
    double overallRQEPShortestLifetime = 0;
    while(alive)
    {
      ChannelModel channelModel = 
        new ChannelModel(rQEP.getLogicalOverlayNetwork(), rQEP.getUnreliableAgenda(), 
                         manager.getWsnTopology().getMaxNodeID(), manager.getWsnTopology(),
                         manager.getCostsParamters(), robustFolder, seed);
      ArrayList<RunTimeSite> qepCostAveragesResilentQeps = simulateRunOfRQEP(rQEP, channelModel);
      //correct energy model values
      Iterator<RunTimeSite> newQEPCostIterator = qepCostAveragesResilentQeps.iterator();
      while(newQEPCostIterator.hasNext())
      {
        RunTimeSite newQEPCostContainer = newQEPCostIterator.next();
        RunTimeSite oldQEPCostContainer = this.runningSites.get(newQEPCostContainer.toString());
        oldQEPCostContainer.setQepExecutionCost(newQEPCostContainer.getQepExecutionCost());
      }
      
      
      double agendaLength = Agenda.bmsToMs(rQEP.getAgendaIOT().getLength_bms(false))/new Double(1000); // ms to s
      
      //locate weakest node
      Iterator<Node> siteIter = this.network.siteIterator();
      String failedSite = null;
      double shortestLifetime = Double.MAX_VALUE; 
      while (siteIter.hasNext()) 
      {
        Node site = siteIter.next();
        RunTimeSite rSite = runningSites.get(site.getID());
        double currentEnergySupply = rSite.getCurrentEnergy() - rSite.getCurrentAdaptationEnergyCost();
        double siteLifetime = (currentEnergySupply / runningSites.get(site.getID()).getQepExecutionCost());
        boolean useAcquires = SNEEProperties.getBoolSetting(SNEEPropertyNames.WSN_MANAGER_K_RESILENCE_SENSE);
        //uncomment out sections to not take the root site into account
        if (!site.getID().equals(rQEP.getIOT().getRT().getRoot().getID()) &&
           ((useAcquires) ||  (!useAcquires && !((Site) site).isSource())) &&
           !globalFailedNodes.contains(site.getID())) 
        { 
          if(shortestLifetime > siteLifetime)
          {
            if(!((Site) site).isDeadInSimulation())
            {
              shortestLifetime = siteLifetime;
              failedSite = site.getID();
            }
          }
        }
      }
      
      //update runtimeSites energy levels
      updateSitesEnergyLevels(shortestLifetime, globalFailedNodes);
      overallRQEPShortestLifetime += (shortestLifetime * agendaLength);
      globalFailedNodes.add(failedSite);
      if(rQEP.getLogicalOverlayNetwork().canAdapt(failedSite, rQEP))
      {
        System.out.println("node " + failedSite);
        ArrayList<String> failedNodeIDs = new ArrayList<String>();
        failedNodeIDs.add(failedSite);
        LogicalOverlayStrategy logicalOverlayGenerator = 
          new LogicalOverlayStrategy(this.manager, this._metadata, this.manager.get_metadataManager());  
        List<Adaptation> result = 
          logicalOverlayGenerator.executeHierarchyAdaptation(failedNodeIDs, rQEP.getLogicalOverlayNetwork());
        if(result.size() == 0)
          alive = false;
        rQEP = (RobustSensorNetworkQueryPlan) result.get(0).getNewQep();
        shortestLifetime =  Double.MAX_VALUE;
      }
      else
      {
        alive = false;
      }
    }
    return overallRQEPShortestLifetime;
  }

  private void updateSitesEnergyLevels(double shortestLifetime,
                                       ArrayList<String> globalFailedNodes)
  {
    Iterator<Node> siteIter = this.network.siteIterator();
    shortestLifetime = Math.floor(shortestLifetime);
    while (siteIter.hasNext()) 
    {
      Node site = siteIter.next();
      if(!globalFailedNodes.contains(site.getID()) || globalFailedNodes.get(globalFailedNodes.size() -1).equals(site.getID()))
      {
        RunTimeSite rSite = runningSites.get(site.getID());
        rSite.removeDefinedCost(rSite.getQepExecutionCost() * shortestLifetime);
      }
    }
  }
}
