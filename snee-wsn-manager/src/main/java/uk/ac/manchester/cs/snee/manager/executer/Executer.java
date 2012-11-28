package uk.ac.manchester.cs.snee.manager.executer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.costmodels.HashMapList;
import uk.ac.manchester.cs.snee.compiler.costmodels.cardinalitymodel.CardinalityEstimatedCostModel;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.AutonomicManagerComponent;
import uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.channel.ChannelModel;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.LogicalOverlayNetworkHierarchy;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.RobustSensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

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
  
  
  public Executer(AutonomicManagerImpl autonomicManager)
  {
    manager = autonomicManager;
  }

  public void adapt(Adaptation finalChoice)
  {
    manager.setCurrentQEP(finalChoice.getNewQep());
    manager.updateStrategies(finalChoice);
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
  public void simulateRunOfRQEP(RobustSensorNetworkQueryPlan rQEP, 
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
    //cycle though iterations
    for(int iteration = 0; iteration < numberOfIterations; iteration ++)
    {
      channelModel.runModel(new ArrayList<String>(), rQEP.getIOT());
      Integer tuplesReturned = determineTupleDeliveryRate(channelModel, rQEP);
      Integer maxTuplesReturnable = (int) model.returnAgendaExecutionResult();
      tuplesReturnedFromEachTypeOfQEP.addWithDuplicates(type.RQEP, tuplesReturned);
      tuplesReturnedFromEachTypeOfQEP.addWithDuplicates(type.MAX, maxTuplesReturnable);
      channelModel.clearModel();
    }
  }
  
  /**
   * simulates a run of a number of agenda cycles governed by the 
   * SNEEproperties.wsn_manager.unreliable.channels.simulationLength parameter or 
   * the default value set in SNEEProperties.getSetting().
   * for a Sensor network query plan
   * @param rQEP
   * @param channelModel
   * @throws NumberFormatException
   * @throws SNEEConfigurationException
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  public void simulateRunOfQEP(SensorNetworkQueryPlan QEP, 
                                ChannelModel channelModel)
  throws NumberFormatException, SNEEConfigurationException,
  OptimizationException, SchemaMetadataException, TypeMappingException
  {
    Integer numberOfIterations = Integer.parseInt(SNEEProperties.getSetting(
        SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS_SIMULATION_ITERATIONS));
    numberOfIterations = 50;
    channelModel.setPacketModel(false);
    for(int iteration = 0; iteration < numberOfIterations; iteration ++)
    {
      channelModel.runModel(new ArrayList<String>(), QEP.getIOT());
      Integer tuplesReturned = determineTupleDeliveryRate(channelModel, QEP);
      tuplesReturnedFromEachTypeOfQEP.addWithDuplicates(type.QEP, tuplesReturned);
      channelModel.clearModel();
    }
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
    for(double distanceFactor = 1; distanceFactor >= 0.2; distanceFactor-=0.2)
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
}
