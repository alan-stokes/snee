package uk.ac.manchester.cs.snee.autonomicmanager.anaylsiser;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.autonomicmanager.AutonomicManager;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.graph.EdgeImplementation;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOTUtils;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.IOTPAFUtils;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceExchangePart;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceOperator;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceWhereSchedular;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.CommunicationTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.ExchangePart;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAFUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.compiler.sn.router.Router;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.LinkCostMetric;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Path;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperatorImpl;

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
  private String sep = System.getProperty("file.separator");
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
    //remove faield node
    removedDeadNodeData(newAgenda, wsnTopology, failedNodeID);
    Node failedNode = iot.getNode(failedNodeID);
    //create paf
    PAF paf = createMiniturePAF(agenda, iot, failedNodeID);
    //output fragment PAF
    String sep = System.getProperty("file.separator");
    new IOTPAFUtils(paf).exportAsDotFile(outputFolder.toString() + sep + "fragmentPAF");
    //create new routing tree
    RT routingTree = createNewRoutingTree(agenda, iot, failedNodeID, paf);
    //run fragment paf though where schedular.
    IOT newIOT = dowhereScheduling(paf, routingTree, qep.getCostParameters());
    
    
    return qep;
  }

  private IOT dowhereScheduling(PAF paf, RT rt, CostParameters costParams) 
  throws SNEEException, 
  SchemaMetadataException, 
  OptimizationException, 
  SNEEConfigurationException
  {
    InstanceWhereSchedular instanceWhere = new InstanceWhereSchedular(paf, rt, costParams);
    return instanceWhere.getIOT();
  }

  /**
   * creates a new routeing tree for the where schuedular
   * @param agenda2
   * @param iot2
   * @param failedNodeID
   * @param paf 
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   */
  private RT createNewRoutingTree(AgendaIOT agenda2, IOT iot2,
      int failedNodeID, PAF paf) throws NumberFormatException, SNEEConfigurationException
  {
    Router router = new Router();
    return router.doRouting(paf, "");
  }

  /**
   * creates a fragment of a physical operator tree, this fragment encapsulates the failed nodes operators.
   * @param agenda2
   * @param iot
   * @param failedNodeID
   * @throws SNEEException
   * @throws SchemaMetadataException
   * @throws SNEEConfigurationException
   */
  private PAF createMiniturePAF(AgendaIOT agenda2, IOT iot, int failedNodeID) throws SNEEException, SchemaMetadataException, SNEEConfigurationException
  {
    PAF paf = null;
    //get node and parent node
    Node failedNode = iot.getNode(failedNodeID);
    Site failedNodeSite = (Site)failedNode;
    Node parentNode = failedNode.getOutput(0);
    //get children nodes
    Node [] children = failedNode.getInputs();
    
    //get parent first operator.
    
    ArrayList<InstanceOperator> instanceOperatorsParent = 
      iot.getOpInstances((Site) failedNode, TraversalOrder.PRE_ORDER, true);
    InstanceOperator parentInstanceOperator = instanceOperatorsParent.get(0);
    parentInstanceOperator = (InstanceOperator) parentInstanceOperator.getOutput(0);
    parentInstanceOperator.setLocked(true);
    String[] pathFrag = outputFolder.toString().split(qep.getQueryName());
    paf = new PAF(parentInstanceOperator, iot.getPAF().getDLAF(), agenda2.getCostParameters(), qep.getQueryName() + pathFrag[1]);
    
    //get operators on failed node
    ArrayList<InstanceOperator> instanceOperatorsNode = 
      iot.getOpInstances((Site) failedNode, TraversalOrder.PRE_ORDER, false);
    Iterator<InstanceOperator> nodeOperatorIterator = instanceOperatorsNode.iterator();
    
    /*TODO needs to look for acquire operators and remove them and any other operator above the acquire 
    which has only the acquire as its only operator. 
    */
    //go though operators
    //if no operators, failed node was a exchange operator, adapt by taking exchangeOperators
    if(!nodeOperatorIterator.hasNext())
    {
      //get operators on failed node
      instanceOperatorsNode = 
        iot.getOpInstances((Site) failedNode, TraversalOrder.PRE_ORDER, true);
      nodeOperatorIterator = instanceOperatorsNode.iterator();
    }
    
    boolean first = true;
    while(nodeOperatorIterator.hasNext())
    {
      InstanceOperator instanceOperator = nodeOperatorIterator.next();
      paf.getOperatorTree().addNode(instanceOperator);
      //correctly wire nodes on dead node correctly
      if(first)
      {
        InstanceOperator previousOperator = getPreviousOperator((InstanceOperator)instanceOperator.getOutput(0), instanceOperator.getSite());
        paf.getOperatorTree().addEdge(instanceOperator, previousOperator);
        first = false;
      }
      //go though children nodes linkign them to correct sensornet operator.
      InstanceOperator [] childrenOperators = instanceOperator.getChildren();
      int childrenOperatorsIndex = 0;
      while(childrenOperatorsIndex < childrenOperators.length)
      {
        InstanceOperator childInstanceOperator = childrenOperators[childrenOperatorsIndex];
        InstanceOperator child = getNextOperator(childInstanceOperator, instanceOperator.getSite());
        if(child.getSite().getID().equals(failedNodeSite.getID()))
          child.setLocked(false);
        else
          child.setLocked(true);
        paf.getOperatorTree().addNode(child);
        paf.getOperatorTree().addEdge(child, instanceOperator);
        childrenOperatorsIndex++; 
      }
    }
    correctOperatorTreeLinks(paf);
    return paf;
  }

  
  private void correctOperatorTreeLinks(PAF paf)
  {
    ArrayList<Node> nodes = new ArrayList<Node>(paf.getOperatorTree().getNodes());
    Iterator<Node> nodeIterator = nodes.iterator();
    while(nodeIterator.hasNext())
    {
      InstanceOperator node = (InstanceOperator) nodeIterator.next();
      node.removeAllInputs();
    }
    nodeIterator = nodes.iterator();
    while(nodeIterator.hasNext())
    {
      InstanceOperator node = (InstanceOperator) nodeIterator.next();
      HashSet<EdgeImplementation> edges = paf.getOperatorTree().getNodeEdges(node.getID());
      Iterator<EdgeImplementation> edgeIterator = edges.iterator();
      while(edgeIterator.hasNext())
      {
        EdgeImplementation edge = edgeIterator.next();
        if(edge.getDestID() == node.getID())
        {
          String sourceID =  edge.getSourceID();
          InstanceOperator source = (InstanceOperator) paf.getOperatorTree().getNode(sourceID);
          node.addInput(source);
        }
      }
    }  
  }

  public InstanceOperator getPreviousOperator(InstanceOperator current,  Site currentSite)
  {
    boolean notFound = true;
    while(notFound)
    {
      if(!(current instanceof InstanceExchangePart) || 
         !(current.getSite().getID().equals(currentSite.getID())))
      {
        return current;
      }
      else
      {
        current = (InstanceOperator) current.getOutput(0);
      }
    }
    return null;
  }
  
  public InstanceOperator getNextOperator(InstanceOperator current, Site currentSite)
  {
    boolean notFound = true;
    while(notFound)
    {
      if(!(current instanceof InstanceExchangePart) || 
         !(current.getSite().getID().equals(currentSite.getID())))
      {
        return current;
      }
      else
      {
        current = (InstanceOperator) current.getInput(0);
      }
    }
    return null;
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
                                   int failedNodeID) 
  throws OptimizationException
  {
    //remove dead node from new agenda, topology, new iot(so that adjustments can be made)
    newAgenda.removeNodeFromAgenda(failedNodeID);
    Node failedNode = iot.getNode(failedNodeID);
    outputTopologyAsDotFile("/topologyAfterNodeLoss.dot");
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
      agendaIOT.setID("newAgenda");
      AgendaIOTUtils output = new AgendaIOTUtils(agendaIOT, newIOT, true);
      File agendaFolder = new File(outputFolder.toString() + sep + "Agendas");
      agendaFolder.mkdir();
      output.generateImage(agendaFolder.toString());
      output.exportAsLatex(agendaFolder.toString() + sep, "newAgenda");
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
  
  /**
   * method used to output topology as a dot file.
   * @param string location and name of topology file.
   */
  private void outputTopologyAsDotFile(String string)
  {
    try
    {
      
      File topFolder = new File(outputFolder.toString() + sep + "Topology");
      topFolder.mkdir();
      this.wsnTopology.exportAsDOTFile(topFolder.toString() + string);
    }
    catch (SchemaMetadataException e)
    {
      e.printStackTrace();
    }
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
