package uk.ac.manchester.cs.snee.manager.anayliser;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.Adapatation;
import uk.ac.manchester.cs.snee.manager.AutonomicManager;
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

public abstract class FrameWorkAbstract
{
  protected SensorNetworkQueryPlan qep;
  protected AutonomicManager manager;
  protected SourceMetadataAbstract _metadata;
  protected File outputFolder;
  protected String sep = System.getProperty("file.separator");
  
  /**
   * bog standard constructor
   * @param manager
   */
  public FrameWorkAbstract(AutonomicManager manager, SourceMetadataAbstract _metadata)
  {
    this.manager = manager;
    this._metadata = _metadata;
  }
  /**
   * checks that the framework can adapt to one failed node
   * @param failedNode
   * @return
   */
  public abstract boolean canAdapt(String failedNode);
  /**
   * checks that the framework can adapt to all the failed nodes
   * @param failedNodes
   * @return
   */
  public abstract boolean canAdaptToAll(ArrayList<String> failedNodes);
  /**
   * used to set up a framework
   * @param oldQep
   * @param noTrees
   */
  public abstract void initilise(QueryExecutionPlan oldQep, Integer noTrees)
  throws SchemaMetadataException ;
  /**
   * calculates a set of adpatations which will produce new QEPs which respond to the 
   * failed node. 
   * @param nodeID the id for the failed node of the query plan
   * @return new query plan which has now adjusted for the failed node.
   */
  public abstract List<Adapatation> adapt(ArrayList<String> failedNodes)  
  throws OptimizationException, SchemaMetadataException, 
  TypeMappingException, AgendaException, 
  SNEEException, SNEEConfigurationException, 
  MalformedURLException, MetadataException, 
  UnsupportedAttributeTypeException, 
  SourceMetadataException, TopologyReaderException, 
  SNEEDataSourceException, CostParametersException, 
  SNCBException, NumberFormatException, SNEECompilerException;
  
  
  /**
   * helper method to get topology from the qep
   * @return topology
   */
  public Topology getWsnTopology()
  {
//    qep.getMetaData().getOutputAttributes().get(0).getExtentName()_metadata.getSource(qep.getMetaData())
    Set<SourceMetadataAbstract> sourceSets = qep.getDLAF().getSources();
    SensorNetworkSourceMetadata sm;
    if(sourceSets.size() == 1)
    {
      Iterator<SourceMetadataAbstract> sourceIterator = sourceSets.iterator();
      sm = (SensorNetworkSourceMetadata) sourceIterator.next();
      Topology network = sm.getTopology();
      return network;
    }
    else
    {
      System.out.println("error, more than 1 network");
      return null;
    }
  }
  
}
