package uk.ac.manchester.cs.snee.manager.planner;

import java.io.IOException;
import java.util.List;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.manager.AutonomicManager;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;

public class Planner 
{

  private AutonomicManager manager;
  private ChoiceAssessor assessor;
  
  public Planner(AutonomicManager autonomicManager, SourceMetadataAbstract _metadata, MetadataManager _metadataManager)
  {
    manager = autonomicManager;
    assessor = new ChoiceAssessor(_metadata, _metadataManager, manager.getOutputFolder());
  }

  /**
   * takes a set of adaptations and assesses each one for energy and time costs of executing the adaptation.
   * @param choices
   * @return
   * @throws IOException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws CodeGenerationException 
   */
  public Adaptation assessChoices(List<Adaptation> choices) 
  throws 
  IOException, OptimizationException, 
  SchemaMetadataException, TypeMappingException, 
  CodeGenerationException
  {
    return assessor.assessChoices(choices);
  }
  
}
