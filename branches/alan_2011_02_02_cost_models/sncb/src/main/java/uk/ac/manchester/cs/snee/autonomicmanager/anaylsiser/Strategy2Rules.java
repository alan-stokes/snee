package uk.ac.manchester.cs.snee.autonomicmanager.anaylsiser;

import uk.ac.manchester.cs.snee.autonomicmanager.AutonomicManager;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.costmodels.IOT;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaLengthException;
import uk.ac.manchester.cs.snee.compiler.queryplan.CommunicationTask;
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
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
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
  private Agenda agenda;
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
    this.agenda = this.qep.getAgenda();
  }
  
  /**
   * calculates new QEP designed to adjust for the failed node. 
   * @param nodeID the id for the failed node of the query plan
   * @return new query plan which has now adjusted for the failed node.
   */
  public SensorNetworkQueryPlan calculateNewQEP(int failedNodeID)
  { 
    //create a duplicate agenda
    Agenda newAgenda = AgendaCopy(agenda);
    //remove dead node from new agenda & topology(so that adjustments can be made)
    newAgenda.removeNodeFromAgenda(failedNodeID);
    Node failedNode = iot.getNode(failedNodeID);
    wsnTopology.removeNode(failedNode.getID());
    outputTopologyAsDotFile("topologyAfterNodeLoss.dot");
    //collect children and parents nodes to failed node
    ArrayList<Node> children = new ArrayList<Node>(failedNode.getInputsList());
    Node parent = failedNode.getOutput(0);
    //check if theres a route between children and parent
    if(!checkRouteExistsInTopology(children, parent))
    {
      System.out.println("no link between a child and parent, currently failed");
      return qep;
    }
    //set up children to be in order of transmission tasks
    newAgenda.orderNodesByTransmissionTasks(children);
    //all checks done, start calculation
    childrenReWiring(children, parent, newAgenda, failedNode);
    //tempralCorrection(parent, )
    return qep;
  }

  /**
   * 
   * @param children the children to node failedNode
   * @param parent the parent of failedNode
   * @param newAgenda the agenda all changes are made to
   * @param failedNode the failed node
   * function designed to connect all children of faield node to parent and update Agenda accordingly.
   */
  private void childrenReWiring(ArrayList<Node> children, Node parent,
      Agenda newAgenda, Node failedNode)
  {
    long time  = 0;
    long timePrime = 0;
    LinkCostMetric linkCostMetric = LinkCostMetric.RADIO_LOSS;
    Iterator<Node> childIterator = children.iterator();
    while(childIterator.hasNext())
    {
      Node child = childIterator.next();
      Path route = wsnTopology.getShortestPath(child.getID(), parent.getID(), linkCostMetric);
      Iterator<Site> routeIterator = route.iterator();
      routeIterator.next();
      Node nodePrime = routeIterator.next();
      CommunicationTask lastRecieve = null;
      if((lastRecieve = newAgenda.getLastCommunicationTask(nodePrime, CommunicationTask.RECEIVE)) != null)
      {
        CommunicationTask oldCommTask = agenda.getTransmissionTask(child);
      }
      else
      {
        
      }
    }
    
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
   * @param orginal 
   * @return the copyied version of orginal
   */
  private Agenda AgendaCopy(Agenda orginal) 
  {
    Agenda agenda = null;
    //Cloner cloner=new Cloner();
    //agenda = cloner.deepClone(orginal);
    try
    {
      agenda = new Agenda(orginal.getAcquisitionInterval_bms(), orginal.getBufferingFactor(),
                          orginal.getDAF(), orginal.getCostParameters(), "new",
                          false);
    }
    catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return agenda;
  }
}
