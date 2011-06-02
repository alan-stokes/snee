package uk.ac.manchester.cs.snee.autonomicmanager.anaylsiser;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.autonomicmanager.AutonomicManager;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOTUtils;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceExchangePart;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.CommunicationTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePart;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.LinkCostMetric;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Path;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

/**
 * @author stokesa6
 *class AnaylsiserStrategy2Rules encapsulates the rules designed to calculate 
 *what possible changes the autonomic manager can do to adjust for a failure of a node
 */

public class Strategy2Rules
{
  private AutonomicManager manager;
  private SensorNetworkQueryPlan qep;
  private RT routingTree;
  private Topology wsnTopology;
  private IOT iot;
  private AgendaIOT agenda;
  private File outputFolder;
  /**
   * @param autonomicManager
   * the parent of this class.
   */
  public Strategy2Rules(AutonomicManager autonomicManager)
  {
    this.manager = autonomicManager;
  }
  
  public void initilise(QueryExecutionPlan qep) throws SchemaMetadataException 
  {
    this.qep = (SensorNetworkQueryPlan) qep;
    SensorNetworkSourceMetadata sm = (SensorNetworkSourceMetadata)this.qep.getDLAF().getSource();
    this.wsnTopology =  sm.getTopology();
    outputFolder = manager.getOutputFolder();
    outputTopologyAsDotFile("/topology.dot");
    this.routingTree = this.qep.getRT();
    this.iot = this.qep.getIOT();
    this.agenda = this.qep.getAgenda().getAgendaIOT();
  }
  
  /**
   * calculates new QEP designed to adjust for the failed node. 
   * @param nodeID the id for the failed node of the query plan
   * @return new query plan which has now adjusted for the failed node.
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws AgendaException 
   * @throws SNEEConfigurationException 
   * @throws SNEEException 
   */
  public SensorNetworkQueryPlan calculateNewQEP(int failedNodeID) throws OptimizationException, SchemaMetadataException, TypeMappingException, AgendaException, SNEEException, SNEEConfigurationException
  { 
    outputNewAgendaImage(agenda, iot);
    //create copies of iot and agenda
    AgendaIOT newAgenda = AgendaCopy(agenda);
    IOT newIOT = IOTCopy(iot);
    //remove faield node
    removedDeadNodeData(newAgenda, wsnTopology, newIOT, failedNodeID);
    Node failedNode = iot.getNode(failedNodeID);
    
    //collect children and parents nodes to failed node
    ArrayList<Node> children = new ArrayList<Node>(failedNode.getInputsList());
    Node parent = failedNode.getOutput(0);
    //check if theres a route between children and parent
    if(!checkRouteExistsInTopology(children, parent))
    {
      System.out.println("no link between a child and parent, currently failed");
      return qep;
    }
    
    //set up new fragment location
    fragmentPositioning(children, newAgenda, newIOT, failedNode, parent);
    
    //set up children to be in order of transmission tasks
    newAgenda.orderNodesByTransmissionTasks(children);
    //all checks done, start children rewiring
    childrenReWiringAndRouting(children, parent, newAgenda, newIOT, failedNode);
    //tempralCorrection(parent, )
    outputNewAgendaImage(newAgenda, newIOT);
    return qep;
  }

  /**
   * removes the failed node from all dependent objects
   * @param newAgenda
   * @param wsnTopology2
   * @param newIOT
   * @param failedNodeID
   * @throws OptimizationException
   */
  private void removedDeadNodeData(AgendaIOT newAgenda, Topology wsnTopology2,
      IOT newIOT, int failedNodeID) throws OptimizationException
  {
    //remove dead node from new agenda, topology, new iot(so that adjustments can be made)
    newAgenda.removeNodeFromAgenda(failedNodeID);
    Node failedNode = iot.getNode(failedNodeID);
    newIOT.removeSite((Site)failedNode);
    outputTopologyAsDotFile("/topologyAfterNodeLoss.dot");
  }
  
  /**
   * places any fragments on dead node on node within children path
   * @param children set of children nodes
   * @param newAgenda 
   * @param newIOT
   * @param failedNode the node which has failed in original QEP
   * @param parent parent of failedNode
   */
  private void fragmentPositioning(ArrayList<Node> children, AgendaIOT newAgenda,
      IOT newIOT, Node failedNode, Node parent)
  {
    /*should call where scheduler with reduced scope*/
    
    
  }

  /**
   * outputs a agenda in latex form into the autonomic manager.
   * @param agendaIOT
   * @param newIOT
   */
  private void outputNewAgendaImage(AgendaIOT agendaIOT, IOT newIOT)
  {
    try
    {
      AgendaIOTUtils output = new AgendaIOTUtils(agendaIOT, newIOT, true);
      output.generateImage(outputFolder.toString());
      output.exportAsLatex(outputFolder.toString(), "newAgenda");
    }
    catch (SNEEConfigurationException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } 
    
  }

  /**
   * 
   * @param children the children to node failedNode
   * @param parent the parent of failedNode
   * @param newAgenda the agenda all changes are made to
   * @param newIOT 
   * @param failedNode the failed node
   * function designed to connect all children of failed node to parent and update Agenda and iot accordingly.
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws AgendaException 
   * @throws SNEEConfigurationException 
   * @throws SNEEException 
   */
  private void childrenReWiringAndRouting(ArrayList<Node> children, Node parent,
      AgendaIOT newAgenda, IOT newIOT, Node failedNode) 
  throws OptimizationException, 
         SchemaMetadataException, 
         TypeMappingException, AgendaException, SNEEException, SNEEConfigurationException
  {
    //initil set up phase
    long time  = 0;
    long timePrime = 0;
    LinkCostMetric linkCostMetric = LinkCostMetric.RADIO_LOSS;
    Iterator<Node> childIterator = children.iterator();
    boolean merged = false;
    
    //go though each child
    while(childIterator.hasNext())
    {
      //get route to parent
      Node child = childIterator.next();
      Path route = wsnTopology.getShortestPath(child.getID(), parent.getID(), linkCostMetric);
      
      Iterator<Site> routeIterator = route.iterator();
      routeIterator.next();
      Node nodePrime = routeIterator.next();
      CommunicationTask lastRecieve = null;
      //choose correct time
      if((lastRecieve = newAgenda.getLastCommunicationTask(nodePrime)) != null)
      {
        CommunicationTask oldCommTask = agenda.getTransmissionTask(child);
        long endTimeOfLastRecieve = lastRecieve.getEndTime();
        long startTimeInOrginalAgenda = oldCommTask.getStartTime();
        time = Math.max(endTimeOfLastRecieve, startTimeInOrginalAgenda);
      }
      else
      {
        CommunicationTask oldCommTask = agenda.getTransmissionTask(child);
        long startTimeInOrginalAgenda = oldCommTask.getStartTime();
        time = Math.max(timePrime, startTimeInOrginalAgenda);
      }
      //get communication task from child to failed node
      cleanCommunicationBetweenNodes(child, failedNode, nodePrime, newAgenda, time, newIOT);
      //set up rest of route
      setupRouteWiring(child, failedNode, newAgenda, time, routeIterator, newIOT, parent);
    }
  }

  /**
   * does routing section of notation
   * @param child
   * @param failedNode
   * @param newAgenda
   * @param time
   * @param routeIterator
   * @param newIOT
   * @param parent
   */
  private void setupRouteWiring(Node child, Node failedNode, AgendaIOT newAgenda,
      long time, Iterator<Site> routeIterator, IOT newIOT, Node parent)
  {
    Node nodePrime = routeIterator.next();
    
    
  }

  /**
   * connects two nodes in communication (includes wiring in both agenda and iot)
   * @param child
   * @param failedNode
   * @param nodePrime
   * @param newAgenda
   * @param time
   * @param newIOT
   * @throws AgendaException
   * @throws OptimizationException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws SNEEConfigurationException 
   * @throws SNEEException 
   */
  private void cleanCommunicationBetweenNodes(Node child, Node failedNode,
      Node nodePrime, AgendaIOT newAgenda, long time, IOT newIOT) 
  throws AgendaException, 
         OptimizationException, 
         SchemaMetadataException, 
         TypeMappingException, SNEEException, SNEEConfigurationException
  {
    //remove task
    CommunicationTask failedCommTask = newAgenda.getCommunicationTaskBetween(child, failedNode);
    int childIndex = newAgenda.removeCommunicationTask(failedCommTask);
    //get exchange parts set up
    HashSet<ExchangePart> exchangeComponents = new HashSet<ExchangePart>();
    HashSet<ExchangePart> oldExchangeComponents = failedCommTask.getExchangeComponents();
    Iterator<ExchangePart> partIterator = oldExchangeComponents.iterator();
    while(partIterator.hasNext())
    {
      ExchangePart part = partIterator.next();
      exchangeComponents.add(new ExchangePart(part.getDestFrag(), (Site)child, part.getDestFrag(),
                                              (Site)nodePrime, part.getCurrentSite(), part.getComponentType(),
                                              part.isRemote(), part.getPrevious()));
    }
    //append new task to agenda
    newAgenda.appendCommunicationTask((Site)child, (Site)nodePrime, time, exchangeComponents, childIndex);
    //append new operators to IOT
    InstanceExchangePart childExchangePart = (InstanceExchangePart) newIOT.getRootOperatorOfSite((Site)child);
   // InstanceExchangePart nodePrimeExchangeOperator =       
    //new InstanceExchangePart(childExchangePart.getSourceFrag(), childExchangePart.getSourceSite(),
      //                       );
    //newIOT.addOpInstToSite(opInst, site)
  }

  /**
   * method used to output topology as a dot file.
   * @param string location and name of topology file.
   */
  private void outputTopologyAsDotFile(String string)
  {
    try
    {
      this.wsnTopology.exportAsDOTFile(outputFolder.toString() + string);
    }
    catch (SchemaMetadataException e)
    {
      e.printStackTrace();
    }
  }

  /**
   * 
   * @param children the set of nodes needed as source nodes to path
   * @param parent the node which all routes must reach
   * @return true if route exists between all children and parent
   */
  private boolean checkRouteExistsInTopology(ArrayList<Node> children, Node parent)
  {
    boolean noRoute = false;
    LinkCostMetric linkCostMetric = LinkCostMetric.RADIO_LOSS;
    Iterator<Node> childrenIterator = children.iterator();
    //go though each child checking if theres a route between it and parent
    while(childrenIterator.hasNext() && !noRoute)
    {
      Node child = childrenIterator.next();
      Path path;
      if((path = wsnTopology.getShortestPath(child.getID(), parent.getID(), linkCostMetric)) == null)
      {
        noRoute = true;
      }
    }
    return !noRoute;
  }
  
  /**
   * method to create a deep copy of the orginal agenda 
   * @param agendaIOT 
   * @return the copyied version of orginal
   */
  //TODO fix to make true deep copy
  private AgendaIOT AgendaCopy(AgendaIOT agendaIOT) 
  {
    AgendaIOT agenda = null;
    //Cloner cloner=new Cloner();
    //agenda = cloner.deepClone(orginal);
    try
    {
      agenda = new AgendaIOT(agendaIOT.getAcquisitionInterval_bms(), agendaIOT.getBufferingFactor(),
                          iot, agendaIOT.getCostParameters(), this.qep.getQueryName(),
                          false);
    }
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return agenda;
  }
  
  /**
   * method to create a deep copy of the orginal iot 
   * @param orginal
   * @return
   */
  //TODO fix to make true deep copy
  private IOT IOTCopy(IOT orginal)
  {
    IOT iot = null;
    try
    {
      iot = new IOT(orginal.getPAF(), orginal.getRT(), "");
    }
    catch(Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return iot;
  }
  
  
}
