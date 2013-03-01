package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.channel;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.costmodels.HashMapList;
import uk.ac.manchester.cs.snee.compiler.costmodels.avroracosts.AlphaBetaExpression;
import uk.ac.manchester.cs.snee.compiler.costmodels.avroracosts.AvroraCostParameters;
import uk.ac.manchester.cs.snee.compiler.costmodels.cardinalitymodel.CollectionOfPackets;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceFragment;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceFragmentTask;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.CommunicationTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.Fragment;
import uk.ac.manchester.cs.snee.compiler.queryplan.FragmentTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.RadioOnTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.SleepTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.Task;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.LogicalOverlayNetworkHierarchy;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.UnreliableChannelAgenda;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAcquireOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetDeliverOperator;

public class ChannelModel implements Serializable
{
  /**
   * 
   */
  private static final long serialVersionUID = 7449374992445910226L;
  private ArrayList<ChannelModelSite> channelModel = new ArrayList<ChannelModelSite>();
  private LogicalOverlayNetworkHierarchy logicaloverlayNetwork;
  private UnreliableChannelAgenda agenda = null;
  private NoiseModel noiseModel; 
  private CostParameters costs;
  private int iteration = 0;
  private AgendaIOT agendaIOT = null;
  private File executorFolder;
  
  
  /**
   * constructor for channel model
   * @param logicaloverlayNetwork
   * @param agenda
   * @param networkSize
   * @param failedNodes
   * @throws SNEEConfigurationException 
   * @throws IOException 
   */
    public ChannelModel (LogicalOverlayNetworkHierarchy logicaloverlayNetwork,  
                       UnreliableChannelAgenda agenda, int networkSize, Topology network,
                       CostParameters costs, File executorFolder)
  throws SNEEConfigurationException, IOException
  {
    this.agenda = agenda;
    this.executorFolder = executorFolder;
    this.costs = costs;
    this.logicaloverlayNetwork = logicaloverlayNetwork;
    setupEmptyArray(networkSize + 1);
    createChannelSites(false);
    noiseModel = new NoiseModel(network, costs);
  }
  public ChannelModel (LogicalOverlayNetworkHierarchy logicaloverlayNetwork,  
        AgendaIOT agenda, int networkSize, Topology network,
        CostParameters costs, File executorFolder)
  throws SNEEConfigurationException, IOException
  {
    this.agendaIOT = agenda;
    this.executorFolder = executorFolder;
    this.costs = costs;
    this.logicaloverlayNetwork = logicaloverlayNetwork;
    setupEmptyArray(networkSize + 1);
    createChannelSites(false);
    noiseModel = new NoiseModel(network, costs);
  }
    

  public ChannelModel(LogicalOverlayNetworkHierarchy logicaloverlayNetwork,
      AgendaIOT agenda, int networkSize, Topology network,
      CostParameters costs, File executorFolder, Long seed, boolean packetID)
  throws SNEEConfigurationException, IOException
  {
    this.agendaIOT = agenda;
    this.executorFolder = executorFolder;
    this.costs = costs;
    this.logicaloverlayNetwork = logicaloverlayNetwork;
    setupEmptyArray(networkSize + 1);
    createChannelSites(packetID);
    noiseModel = new NoiseModel(network, costs, seed);
    
  }
  /**
   * creates an empry array of nulls for correct placement of channel sites.
   * @param networkSize
   */
  private void setupEmptyArray(int networkSize)
  {
    for(int index = 0; index < networkSize; index++)
    {
      channelModel.add(null);
    }
  }

  /**
   * runs though one iteration of agenda using the unreliable channel model 
   * to determine which tasks within the agenda are to be ran at runtime by using a 
   * noise model
   * @param robustFolder 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   */
  public HashMapList<String, RunTimeSite> runModel(ArrayList<String> failedNodes, IOT IOT) 
  throws OptimizationException, SchemaMetadataException, 
  TypeMappingException, NumberFormatException, SNEEConfigurationException
  {
    System.out.println("running iteration " + iteration);
    Iterator<Task> taskIterator = null;
    if(agenda == null)
     taskIterator =  agendaIOT.taskIteratorOrderedByTime();
    else
      taskIterator = this.agenda.taskIteratorOrderedByTime();
    while(taskIterator.hasNext())
    {
      Task task = taskIterator.next();
      //System.out.println(task.toString());
      if(task instanceof CommunicationTask )
      {
        boolean runable = true;
        CommunicationTask cTask = (CommunicationTask) task;
        if(failedNodes.contains(cTask.getSourceID()) || 
           failedNodes.contains(cTask.getDestID()))
        {
          cTask.setRan(false);
          runable = false;
        }
        
        if(cTask.getMode() == CommunicationTask.RECEIVE && runable)
        {
          if(tranmissionTaskRan(cTask))
           {
            if(isSibling(cTask.getSourceID(), cTask.getTrueDestSite().getID())) 
            {
              //int packetsToTransmit = cTask.getMaxPacektsTransmitted();
              ArrayList<Integer> packetIDsTransmitted = packetsSentByTranmissionTask(cTask, IOT);
              for(int index = 0; index < packetIDsTransmitted.size(); index++)
              {
                Integer packetID = packetIDsTransmitted.get(index);
                tryRecievingSibling(cTask.getTrueDestSite().getID(), Integer.parseInt(cTask.getSourceID()), 
                                    packetID, cTask.getStartTime());
              }
              cTask.setRan(true);
            }
            else
            {
            //  int packetsToTransmit = cTask.getMaxPacektsTransmitted();
               ArrayList<Integer> packetIDsTransmitted = packetsSentByTranmissionTask(cTask, IOT);
               for(int index = 0; index < packetIDsTransmitted.size(); index++)
               {
                 Integer packetID = packetIDsTransmitted.get(index);
                 tryRecieving(cTask.getDestID(), Integer.parseInt(cTask.getSourceID()), packetID, 
                              cTask.getStartTime());
               }
               cTask.setRan(true);
            }
           }
          else
          {
            cTask.setRan(false);
          }
        }
          
        else if(cTask.getMode() == CommunicationTask.TRANSMIT && runable)
        {
          ChannelModelSite source = channelModel.get(Integer.parseInt(cTask.getSourceID()));
          
          if(source.receivedACK())
          {
            cTask.setRan(false);
          }
          else
          {
            cTask.setRan(true);
            source.transmittedPackets();
            source.updateCycle();
          }
        }
        
        else if(cTask.getMode() == CommunicationTask.ACKRECEIVE && runable)
        {
          ChannelModelSite sourceSite = channelModel.get(Integer.parseInt(cTask.getSourceID())); 
          ChannelModelSite destSite = channelModel.get(Integer.parseInt(cTask.getDestNode().getID()));
          
          if(destSite.getTask(cTask.getStartTime(), CommunicationTask.ACKTRANSMIT).isRan())
            if(!didPacketGetRecived(destSite.toString(), Integer.parseInt(sourceSite.toString()),
                cTask.getStartTime(), 1))
              cTask.setRan(false);
            else
            {
              cTask.setRan(true);
              sourceSite.receivedAckFrom(cTask.getCommID());
            }
          else
            cTask.setRan(false);
        }
        
        else if(cTask.getMode() == CommunicationTask.ACKTRANSMIT && runable)
        {
          ChannelModelSite site = channelModel.get(Integer.parseInt(cTask.getDestID())); 
          if(site.needToTransmitAckTo(cTask.getSourceID()))
          {
            cTask.setRan(true);
            site.TransmitAckTo(cTask.getSourceID());
          }
          else
          {
            cTask.setRan(false);
          }
        }
      }
    }
  //  ChannelModelUtils modelUtils = new ChannelModelUtils(channelModel, logicaloverlayNetwork);
    //modelUtils.plotPacketRates(iteration, executorFolder);
    iteration++;
    Set<Site> sites = IOT.getAllSites();
    Iterator<Site> siteIterator = sites.iterator();
    HashMapList<String, RunTimeSite> siteEnergyLevels = new  HashMapList<String, RunTimeSite>();
    while(siteIterator.hasNext())
    {
      Site site = siteIterator.next();
      double qepCost = this.calculateQepEnergyCost(site, IOT);
      RunTimeSite rSite = new RunTimeSite(0.0, site.getID(), qepCost);
      siteEnergyLevels.add(site.getID(), rSite);
    }
    return siteEnergyLevels;
  }
  
  /**
   * method that allows to determine QEPCost of a site given the edge failure model and 
   * therefore what tasks were acutally ran
   * @param site
   * @param iOT
   * @param agendaIOT2
   * @param agenda2
   * @return
   */
  private double calculateQepEnergyCost(Site site, IOT iOT)
  throws OptimizationException, SchemaMetadataException, 
  TypeMappingException, SNEEConfigurationException
  {
    double sumEnergy = 0;
    long cpuActiveTimeBms = 0;
    double sensorEnergy = 0;
    ArrayList<Task> siteTasks = null;
    if(this.agenda != null)
    {
      site =  this.agenda.getSiteByID(site.getID());
      siteTasks = this.agenda.getTasks().get(site);
    }
    else
    {
      site = this.agendaIOT.getSiteByID(site.getID());
      siteTasks = this.agendaIOT.getTasks().get(site);
    }
    
    //not within the QEP. so no cost
    if(siteTasks == null)
    {
      return 0;
    }
    for (int i=0; i<siteTasks.size(); i++) 
    {
      Task t = siteTasks.get(i);
      if (t instanceof SleepTask) 
      {
        continue;
      }
      
      cpuActiveTimeBms += t.getDuration();
      if (t instanceof FragmentTask) {
        FragmentTask ft = (FragmentTask)t;
        Fragment f = ft.getFragment();
        if (f.containsOperatorType(SensornetAcquireOperator.class)) {
          sensorEnergy += AvroraCostParameters.getSensorEnergyCost();
        }
        sumEnergy += sensorEnergy;
      }
      else if(t instanceof InstanceFragmentTask)
      {
        InstanceFragmentTask ft = (InstanceFragmentTask)t;
        InstanceFragment f = ft.getFragment();
        if (f.containsOperatorType(SensornetAcquireOperator.class)) {
          sensorEnergy += AvroraCostParameters.getSensorEnergyCost();
        }
        sumEnergy += sensorEnergy;
      }
      else if (t instanceof CommunicationTask && t.isRan()) {
        CommunicationTask ct = (CommunicationTask)t;
        sumEnergy += getRadioEnergy(ct);
        
      } else if (t instanceof RadioOnTask && t.isRan()) {
        double taskDuration = AgendaIOT.bmsToMs(t.getDuration())/1000.0;
        double radioRXAmp = AvroraCostParameters.getRadioReceiveAmpere(); 
        double voltage = AvroraCostParameters.VOLTAGE;
        double taskEnergy = taskDuration * radioRXAmp * voltage;        
        sumEnergy += taskEnergy;
      }
    }
    sumEnergy += getCPUEnergy(cpuActiveTimeBms);
    return sumEnergy;
  }
  
  private double getRadioEnergy(CommunicationTask ct)
  throws OptimizationException, SchemaMetadataException, 
  TypeMappingException 
  {
    double taskDuration = AgendaIOT.bmsToMs(ct.getDuration())/1000.0;
    double voltage = AvroraCostParameters.VOLTAGE;
    
    double radioRXAmp = AvroraCostParameters.getRadioReceiveAmpere();
    if (ct.getMode()==CommunicationTask.RECEIVE) {
       
      double taskEnergy = taskDuration*radioRXAmp*voltage; 
      return taskEnergy;
    }
    Site sender = ct.getSourceNode();
    Site receiver = (Site)sender.getOutput(0);
    int txPower = 0;
    if(agenda != null)
      txPower = (int)agenda.getIOT().getRT().getRadioLink(sender, receiver).getEnergyCost();
    else
      txPower = (int)agendaIOT.getIOT().getRT().getRadioLink(sender, receiver).getEnergyCost();
    double radioTXAmp = AvroraCostParameters.getTXAmpere(txPower);
    
    Integer noPackets = 0;
    if(agenda != null)
      noPackets = 
      this.channelModel.get(Integer.parseInt(ct.getSite().getID())).getPacketIds().size();
    else
      noPackets = 
        this.channelModel.get(Integer.parseInt(ct.getSite().getID())).getPacketIds().size();
    
    AlphaBetaExpression txTimeExpr = 
      AlphaBetaExpression.multiplyBy( new AlphaBetaExpression(noPackets),
                                      AvroraCostParameters.PACKETTRANSMIT);
    double txTime = 0;
    if(agenda != null)
      txTime = (txTimeExpr.evaluate(agenda.getAcquisitionInterval_bms(), 
                                    agenda.getBufferingFactor()))/1000.0;
    else
      txTime = (txTimeExpr.evaluate(agendaIOT.getAcquisitionInterval_bms(), 
          agendaIOT.getBufferingFactor()))/1000.0;
    double rxTime = taskDuration-txTime;
    assert(rxTime>=0);
    
    double txEnergy = txTime*radioTXAmp*voltage; 
    double rxEnergy = rxTime*radioRXAmp*voltage; 
    return (txEnergy+rxEnergy); 
  }
  /**
   * calcualtes the cpu energy cost.
   * @param cpuActiveTimeBms
   * @return
   */
  private double getCPUEnergy(long cpuActiveTimeBms)
  {
    double agendaLength = 0;
    if(agenda != null)
      agendaLength = AgendaIOT.bmsToMs(agenda.getLength_bms(false))/1000.0; //bms to ms to s
    else
      agendaLength = AgendaIOT.bmsToMs(agendaIOT.getLength_bms(false))/1000.0; //bms to ms to s
    double cpuActiveTime = AgendaIOT.bmsToMs(cpuActiveTimeBms)/1000.0; //bms to ms to s
    double cpuSleepTime = agendaLength - cpuActiveTime; // s
    double voltage = AvroraCostParameters.VOLTAGE;
    double activeCurrent = AvroraCostParameters.CPUACTIVEAMPERE;
    double sleepCurrent = AvroraCostParameters.CPUPOWERSAVEAMPERE;
    //double sleepCurrent =  AvroraCostParameters.CPUIDLEAMPERE;
    
    double cpuActiveEnergy = cpuActiveTime * activeCurrent * voltage; //J
    double cpuSleepEnergy = cpuSleepTime * sleepCurrent * voltage; //J
    //return cpuActiveEnergy;
    return cpuActiveEnergy + cpuSleepEnergy;
  }
  /**
   * checks if a the transmission task thats linked to this recieve task (CTask) has ran
   * @param cTask
   * @return
   */
  private boolean tranmissionTaskRan(CommunicationTask cTask)
  {
    ChannelModelSite site = channelModel.get(Integer.parseInt(cTask.getSourceID())); 
    return site.getTask(cTask.getStartTime(), CommunicationTask.TRANSMIT).isRan();
  }
  
  /**
   * determines how many packets were actually sent by the child (based on its recieve)
   * @param cTask
   * @return
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  private ArrayList<Integer> packetsSentByTranmissionTask(CommunicationTask cTask, IOT IOT)
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    ChannelModelSite site = channelModel.get(Integer.parseInt(cTask.getSourceID()));
    if(this.logicaloverlayNetwork.isClusterHead(cTask.getSourceID()))
      return  site.transmittablePackets(IOT);
    else
      return site.transmittablePackets();
  }
  
  public Integer packetsToBeSentByDeliveryTask(Site rootSite, IOT IOT)
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    ChannelModelSite site = channelModel.get(Integer.parseInt(rootSite.getID())); 
    CollectionOfPackets col = site.getTransmitableWindows();
    return site.transmittablePackets(IOT).size();
  }
  
  

  /**
   * determines if a node is in the same logical node
   * @param sourceID
   * @param destID
   * @return
   */
  private boolean isSibling(String sourceID, String destID)
  {
    ArrayList<String> equivNodes = 
      this.logicaloverlayNetwork.getEquivilentNodes(
          this.logicaloverlayNetwork.getClusterHeadFor(sourceID));
    if(equivNodes.contains(destID) || 
       this.logicaloverlayNetwork.getClusterHeadFor(sourceID).equals(destID))
      return true;
    else
      return false;
  }

  /**
   * determines if a node heard any of their 
   * siblings which were of higher priority to the given node transmit
   * @param site
   * @param redundant
   * @return
   */
  public boolean heardHigherPriorityNodeTransmit(ChannelModelSite site, boolean redundant)
  {
    if(site.heardSiblings() == 0)
      return false;
    else
      return true;
  }

  /**
   * checks with noise model to see if a node has been recieved
   * @param destID
   * @param sourceID
   * @param packetID
   * @param startTime
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   */
  private void tryRecieving(String destID, Integer sourceID, int packetID, long startTime)
  throws NumberFormatException, SNEEConfigurationException
  {
    ChannelModelSite outputSite = this.channelModel.get(Integer.parseInt(destID));
    if(agenda == null)
      startTime = startTime +(iteration * this.agendaIOT.getLength_bms(false));
    else
      startTime = startTime +(iteration * agenda.getLength_bms(false));
    startTime = startTime + new Double(Math.ceil(costs.getSendPacket() * (packetID - 1))).longValue();
    if(didPacketGetRecived(destID, sourceID, startTime, packetID))
    {
      outputSite.recivedInputPacket(sourceID.toString(), packetID);
    }
  }
  
  /**
   * checks with noise model to see if a node has been recieved
   * @param destID
   * @param sourceID
   * @param packetID
   * @param startTime
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   */
  private void tryRecievingSibling(String destID, Integer sourceID, int packetID, long startTime)
  throws NumberFormatException, SNEEConfigurationException
  {
    ChannelModelSite outputSite = this.channelModel.get(Integer.parseInt(destID));
    if(agenda == null)
      startTime = startTime +(iteration * this.agendaIOT.getLength_bms(false));
    else
      startTime = startTime +(iteration * agenda.getLength_bms(false));
    startTime = startTime + new Double(Math.ceil(costs.getSendPacket() * (packetID - 1))).longValue();
    if(didPacketGetRecived(destID, sourceID, startTime, packetID))
    {
      outputSite.recivedSiblingPacket(sourceID.toString(), packetID);
    }
  }

  /**
   * checks with the noise model to see if the packet was recieved at the destimation.
   * @param destID
   * @param timeOfTransmission 
   * @param siteID
   * @return
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   */
  private boolean didPacketGetRecived(String destID, Integer sourceID, long timeOfTransmission,
                                      Integer packetID)
  throws NumberFormatException, SNEEConfigurationException
  {
    return noiseModel.packetRecieved(sourceID.toString(), destID, timeOfTransmission,
                                     channelModel.get(Integer.parseInt(destID)), packetID);
  }

  /**
   * helper method for the constructor, creates the models sites with correct parents etc.
   * @param packetID 
   */
  private void createChannelSites(boolean packetID)
  {
    Iterator<String> siteIDIterator = logicaloverlayNetwork.siteIdIterator();
    while(siteIDIterator.hasNext())
    {
      String siteID = siteIDIterator.next();
      Integer siteIDInt = Integer.parseInt(siteID);
      String clusterHeadID = logicaloverlayNetwork.getClusterHeadFor(siteID);
      Iterator<Node> inputNodes = 
        logicaloverlayNetwork.getQep().getRT().getSite(clusterHeadID).getInputsList().iterator();
      HashMap<String, Integer> expectedPackets = new  HashMap<String, Integer>();
      while(inputNodes.hasNext())
      {
        Node input = inputNodes.next();
        Node agendaInput = null;
        if(agenda == null)
          agendaInput = agendaIOT.getSiteByID((Site) input);
        else
          agendaInput = agenda.getSiteByID((Site) input);
        CommunicationTask task = null;
        if(agenda == null)
          task = agendaIOT.getTransmissionTask(agendaInput);
        else 
          task = agenda.getTransmissionTask(agendaInput);
        int packetsToRecieve = task.getMaxPacektsTransmitted();
        expectedPackets.put(input.getID(), packetsToRecieve);
        Iterator<String> equivNodes =
          logicaloverlayNetwork.getActiveEquivilentNodes(input.getID()).iterator();
        while(equivNodes.hasNext())
        {
          String equivNode = equivNodes.next();
          Node agendaEquivInput = null;
          if(agenda == null)
            agendaEquivInput = agendaIOT.getSiteByID(equivNode);
          else
            agendaEquivInput = agenda.getSiteByID(equivNode);
          if(agenda == null)
            task = agendaIOT.getTransmissionTask(agendaEquivInput);
          else
            task = agenda.getTransmissionTask(agendaEquivInput);
          packetsToRecieve = task.getMaxPacektsTransmitted();
          expectedPackets.put(equivNode, packetsToRecieve);
        } 
      }
      if(logicaloverlayNetwork.getQep().getRT().getRoot().getID().equals(clusterHeadID))
      {//root node with delivery operator, doesnt have a cluster, but needs a channel model site
        ChannelModelSite site = null;
        if(agenda == null)
          site = new ChannelModelSite(expectedPackets, siteID, logicaloverlayNetwork, 0, 
                                      agendaIOT.getTasks().get(agendaIOT.getSiteByID(siteID)),
                                      agendaIOT.getBufferingFactor(), costs, this, packetID);
        else
          site = new ChannelModelSite(expectedPackets, siteID, logicaloverlayNetwork, 0, 
                                      agenda.getTasks().get(agenda.getSiteByID(siteID)),
                                      agenda.getBufferingFactor(), costs, this, packetID);
        channelModel.set(siteIDInt, site);
      }
      else //not the root node
      {
        ChannelModelSite site;
        if(logicaloverlayNetwork.isClusterHead(siteID))//your a cluster head
        {
          if(agenda == null)
            site = 
              new ChannelModelSite(expectedPackets, siteID, logicaloverlayNetwork, 
                                   logicaloverlayNetwork.getPriority(siteID), 
                                   agendaIOT.getTasks().get(agendaIOT.getSiteByID(siteID)),
                                   agendaIOT.getBufferingFactor(), costs, this, packetID);
          else
          site = 
            new ChannelModelSite(expectedPackets, siteID, logicaloverlayNetwork, 
                                 logicaloverlayNetwork.getPriority(siteID), 
                                 agenda.getTasks().get(agenda.getSiteByID(siteID)),
                                 agenda.getBufferingFactor(), costs, this, packetID);
        }
        else //your a sibling in the logical overlay to the cluster head. 
        {
          expectedPackets = new  HashMap<String, Integer>();
          if(agenda == null)
          {
            //siblings only receive packets from cluster head and other siblings
            Site clusterHeadSite = this.agendaIOT.getSiteByID(clusterHeadID);
            int packetsTransmittedByClusterHead = 
              this.agendaIOT.getTransmissionTask(clusterHeadSite).getMaxPacektsTransmitted();
            expectedPackets.put(clusterHeadID, packetsTransmittedByClusterHead);
            Iterator<String> siblings = 
              this.logicaloverlayNetwork.getActiveEquivilentNodes(clusterHeadID).iterator();
            while(siblings.hasNext())
            {
              expectedPackets.put(siblings.next(), packetsTransmittedByClusterHead);
            }
            
            site = new ChannelModelSite(expectedPackets, siteID, logicaloverlayNetwork, 
                                       logicaloverlayNetwork.getPriority(siteID), 
                                       agendaIOT.getTasks().get(agendaIOT.getSiteByID(siteID)),
                                       agendaIOT.getBufferingFactor(), costs, this, packetID);
          }
          else
          {
            //siblings only receive packets from cluster head and other siblings
            Site clusterHeadSite = this.agenda.getSiteByID(clusterHeadID);
            int packetsTransmittedByClusterHead = 
              this.agenda.getTransmissionTask(clusterHeadSite).getMaxPacektsTransmitted();
            expectedPackets.put(clusterHeadID, packetsTransmittedByClusterHead);
            Iterator<String> siblings = 
              this.logicaloverlayNetwork.getActiveEquivilentNodes(clusterHeadID).iterator();
            while(siblings.hasNext())
            {
              expectedPackets.put(siblings.next(), packetsTransmittedByClusterHead);
            }
            site = new ChannelModelSite(expectedPackets, siteID, logicaloverlayNetwork, 
                                        logicaloverlayNetwork.getPriority(siteID), 
                                        agenda.getTasks().get(agenda.getSiteByID(siteID)),
                                        agenda.getBufferingFactor(), costs, this, packetID);
          }
        }
          
        channelModel.set(siteIDInt, site);
      }
    }
  }

  public int packetToTupleConversion(Integer packets, Site rootSite)
  throws SchemaMetadataException, TypeMappingException
  {
    Site iotSite = logicaloverlayNetwork.getQep().getIOT().getSiteFromID(rootSite.getID());
    ArrayList<InstanceOperator> operators = logicaloverlayNetwork.getQep().getIOT().getOpInstances(iotSite);
    Iterator<InstanceOperator> opIterator = operators.iterator();
    InstanceOperator rootOp = null;
    while(opIterator.hasNext())
    {
      InstanceOperator opToCheck = opIterator.next();
      if(opToCheck.getSensornetOperator() instanceof SensornetDeliverOperator)
    	  rootOp = opToCheck;
    }
    ChannelModelSite site = channelModel.get(Integer.parseInt(rootSite.getID()));
    return CollectionOfPackets.determineNoTuplesFromWindows(site.getTransmitableWindows().getWindowsOfExtent(rootOp.getExtent()));
  }

  public void clearModel()
  {
    Iterator<ChannelModelSite> modelSiteIterator = channelModel.iterator();
    while(modelSiteIterator.hasNext())
    {
      ChannelModelSite site = modelSiteIterator.next();
      if(site != null)
        site.clearDataStoresForNewIteration();
    }
  }
  
  public void setPacketModel(boolean reliableChannelQEP)
  {
    Iterator<ChannelModelSite> modelSiteIterator = channelModel.iterator();
    while(modelSiteIterator.hasNext())
    {
      ChannelModelSite site = modelSiteIterator.next();
      if(site != null)
        ChannelModelSite.setReliableChannelQEP(reliableChannelQEP);
    }
  }
  
  public ArrayList<ChannelModelSite> getChannelModel()
  {
    return channelModel;
  }
  
  public int determineAggregationTupleContribtuion(IOT iot) 
  throws NumberFormatException, SchemaMetadataException, TypeMappingException
  {
    ChannelModelSite.resetAggreData();
    Iterator<Site> siteIterator = iot.getRT().siteIterator(TraversalOrder.POST_ORDER);
    while(siteIterator.hasNext())
    {
      Site site = siteIterator.next();
      this.channelModel.get(Integer.parseInt(site.getID())).determineAggregationContribution(site.getID());
    }
    return ChannelModelSite.getTuplesParticipatingInAggregation();
  }
}
