package uk.ac.manchester.cs.snee.manager.planner;

import java.io.IOException;
import java.util.List;

import uk.ac.manchester.cs.snee.manager.Adaptation;
import uk.ac.manchester.cs.snee.manager.AutonomicManager;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;

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
   */
  public Adaptation assessChoices(List<Adaptation> choices) 
  throws IOException
  {
    return assessor.assessChoices(choices);
  }
  
}
