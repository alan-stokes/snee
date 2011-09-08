package uk.ac.manchester.cs.snee.manager.planner;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;

public class Planner 
{

  private AutonomicManagerImpl manager;
  private ChoiceAssessor assessor;
  private HashMap<String, RunTimeSite> runningSites;
  
  public Planner(AutonomicManagerImpl autonomicManager, SourceMetadataAbstract _metadata, MetadataManager _metadataManager)
  {
    manager = autonomicManager;
    assessor = new ChoiceAssessor(_metadata, _metadataManager, manager.getOutputFolder());
    runningSites = manager.getRunningSites();
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
    assessor.assessChoices(choices, runningSites);
    Adaptation bestChoice = chooseBestAdaptation(choices);
    String id = manager.getQueryName() + "-" + manager.getAdaptionCount();
    new PlannerUtils(choices).printLatexDocument(bestChoice, id);
    return bestChoice;
  }

  private Adaptation chooseBestAdaptation(List<Adaptation> choices)
  {
    Adaptation finalChoice = null;
    Double cost = Double.MIN_VALUE;
    Iterator<Adaptation> choiceIterator = choices.iterator();
    //calculate each cost, and compares it with the best so far, if the same, store it 
    while(choiceIterator.hasNext())
    {
      Adaptation choice = choiceIterator.next();
      Double choiceCost = choice.getLifetimeEstimate();
      if(choiceCost > cost)
      {
        finalChoice = choice;
        cost = choiceCost;
      }
    }
    return finalChoice;
  }

  public void updateStorageLocation(File outputFolder)
  {
    this.assessor.updateStorageLocation(outputFolder);
  }
  
}
