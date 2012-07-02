package uk.ac.manchester.cs.snee.manager.planner.overlaysuccessorrelation.tabu;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.WhenSchedulerException;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.planner.common.OverlaySuccessor;
import uk.ac.manchester.cs.snee.manager.planner.overlaysuccessorrelation.alternativegenerator.OverlayAlternativeGenerator;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;

public class OverlayNeighbourhoodGenerator
{
  private OverlayTABUList tabuList;
  private int AspirationPlusBounds = 30;
  private OverlayAlternativeGenerator neighbourhoodGenerator;
  
  
  public OverlayNeighbourhoodGenerator(OverlayTABUList tabuList, AutonomicManagerImpl autonomicManager, 
                                MetadataManager _metaManager, 
                                HashMap<String, RunTimeSite> runningSites,
                                SourceMetadataAbstract _metadata, File outputFolder)
  {
    this.tabuList = tabuList;
    neighbourhoodGenerator = 
      new OverlayAlternativeGenerator(outputFolder, _metadata, autonomicManager, _metaManager);
  }
  
  /**
   * generates the neighbourhood to be assessed. 
   * Uses aspiration plus to reduce size of candidates in the neighbourhood.
   * @param currentBestSuccessor 
   * @param position
   * @return
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws IOException 
   * @throws WhenSchedulerException 
   * @throws SNEEException 
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   * @throws CodeGenerationException 
   */
  public ArrayList<OverlaySuccessor> generateNeighbourHood(OverlaySuccessor currentBestSuccessor,
                                                     int position, int iteration) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException, 
  IOException, NumberFormatException, SNEEConfigurationException, SNEEException, 
  WhenSchedulerException, CodeGenerationException
  {
    ArrayList<OverlaySuccessor> neighbourHood = new ArrayList<OverlaySuccessor>();
    neighbourHood =
      neighbourhoodGenerator.generateAlternatives(currentBestSuccessor, 
                                                  AspirationPlusBounds,
                                                  tabuList, position);
    return neighbourHood;
  }
}
