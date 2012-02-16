package uk.ac.manchester.cs.snee.manager.common;

import java.io.File;
import java.io.Serializable;

import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;

public abstract class AutonomicManagerComponent implements Serializable
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -5477565839619065406L;
  
  protected AutonomicManagerImpl manager;
  protected SourceMetadataAbstract _metadata;
  protected String sep = System.getProperty("file.separator");
  protected SensorNetworkQueryPlan qep;
  
  /**
   * helper method to delete folders
   * @param file
   */
  public void deleteFolder(File file)
  {
    manager.deleteFileContents(file);
  }
  
  /**
   * used to set qep for numerous event handling
   * @param qep
   */
  public void setQEP(SensorNetworkQueryPlan qep)
  {
    this.qep = qep;
  }
  
  /**
   * helper method to get topology from the qep
   * @return topology
   */
  public Topology getWsnTopology()
  {
    SensorNetworkSourceMetadata metadata = (SensorNetworkSourceMetadata) _metadata;
    Topology network = metadata.getTopology();
    return network;
  }
}
