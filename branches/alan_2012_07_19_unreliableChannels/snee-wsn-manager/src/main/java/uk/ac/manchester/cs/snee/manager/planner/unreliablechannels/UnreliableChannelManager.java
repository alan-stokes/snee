package uk.ac.manchester.cs.snee.manager.planner.unreliablechannels;

import java.io.File;
import java.io.IOException;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.AgendaException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.AgendaLengthException;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.AutonomicManagerComponent;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.LogicalOverlayStrategy;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.logicaloverlaynetworkgenerator.LogicalOverlayNetwork;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.improved.UnreliableChannelAgendaReduced;
import uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.improved.UnreliableChannelAgendaReducedUtils;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;

/**
 *manager for the unreliable channels aspect of the research.
 *generates an overlay over the network 
 *(using the failed node logical overlay network strategy) and then 
 *develops an agenda that abstracts over communication of physical nodes
 *by use of logical nodes
 * 
 * @author Alan Stokes
 */

public class UnreliableChannelManager extends AutonomicManagerComponent
{

  /**
   * serial id
   */
  
  private MetadataManager _metadataManager;
  private static final long serialVersionUID = 8101482486138925839L;
  private File outputFolder = null;
  
  /**
   * Initialised the UnreliableChannelManager with all necessary links to higher up data
   * @param autonomicManager
   * @param _metadata
   * @param _metadataManager
   */
  public UnreliableChannelManager(AutonomicManagerImpl autonomicManager, 
                                  SourceMetadataAbstract _metadata,  
                                  MetadataManager _metadataManager,
                                  File outputFolder)
  {
    this._metadata = _metadata;
    this.manager = autonomicManager;
    this._metadataManager = _metadataManager;
    this.outputFolder = outputFolder;
  }
  
  /**
   * given a bob standard QEP, turns it into robust qep 
   * by using a logical over and a new agenda system
   * @param qep
   * @return
   * @throws CodeGenerationException 
   * @throws SNEEConfigurationException 
   * @throws IOException 
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws SNEEException 
   * @throws AgendaLengthException 
   * @throws AgendaException 
   */
  public RobustSensorNetworkQueryPlan generateEdgeRobustQEP(SensorNetworkQueryPlan qep, 
                                                            Topology network)
  throws SchemaMetadataException, TypeMappingException, OptimizationException, 
         IOException, SNEEConfigurationException, CodeGenerationException,
         AgendaException, AgendaLengthException, SNEEException
  {
    try{
    LogicalOverlayStrategy overlayGenerator = new LogicalOverlayStrategy(manager, _metadata, _metadataManager);
    overlayGenerator.initilise(qep, 1);
    LogicalOverlayNetwork logicaloverlayNetwork = overlayGenerator.getLogicalOverlay();
    boolean allowDiscontinuousSensing = SNEEProperties.getBoolSetting(
        SNEEPropertyNames.ALLOW_DISCONTINUOUS_SENSING);
    UnreliableChannelAgendaReduced overlayAgenda = 
      new UnreliableChannelAgendaReduced(logicaloverlayNetwork, logicaloverlayNetwork.getQep(), 
                                         network, allowDiscontinuousSensing);
    new UnreliableChannelAgendaReducedUtils(overlayAgenda, logicaloverlayNetwork.getQep().getIOT(), 
                                     true).generateImage(outputFolder.toString());
    return new RobustSensorNetworkQueryPlan(overlayAgenda.getActiveLogicalOverlay().getQep(), 
                                            overlayAgenda.getActiveLogicalOverlay(), overlayAgenda);
    }
    catch(Exception e)
    {
      System.out.println("something broke :" + e.getMessage());
      e.printStackTrace();
      return null;
    }
  }
}
