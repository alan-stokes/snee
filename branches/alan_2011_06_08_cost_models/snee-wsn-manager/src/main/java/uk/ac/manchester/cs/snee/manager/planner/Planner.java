package uk.ac.manchester.cs.snee.manager.planner;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.AdaptationCollection;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.common.StrategyID;
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
  public Adaptation assessChoices(AdaptationCollection choices) 
  throws 
  IOException, OptimizationException, 
  SchemaMetadataException, TypeMappingException, 
  CodeGenerationException
  {
    Adaptation bestOverall = null;
    Adaptation orginal = null;
    try
    {
      Adaptation bestPartial = null;
      Adaptation bestLocal = null;
      List<Adaptation> partialAds = choices.getPartialAdaptations();
      List<Adaptation> localAds = choices.getLocalAdaptations();
      orginal = new Adaptation(choices.getGlobalAdaptation().getOldQep(), StrategyID.Orginal, 1);
      doOrginalAssessment(orginal, choices.getGlobalAdaptation().getOldQep());
      
      //find the best of each framework
      if(!partialAds.isEmpty())
      {
        assessor.assessChoices(partialAds, runningSites);
        bestPartial = chooseBestAdaptation(partialAds);
      }
      
      if(!localAds.isEmpty())
      {
        assessor.assessChoices(localAds, runningSites);
        bestLocal = chooseBestAdaptation(localAds);
      }
      //set up new colelction for the assessor
      List<Adaptation> bestChoices = new ArrayList<Adaptation>();
      if(bestLocal != null)
        bestChoices.add(bestLocal);
      if(bestPartial != null)
        bestChoices.add(bestPartial);
      bestChoices.add(choices.getGlobalAdaptation());
      //assess to find best overall adaptation
      assessor.assessChoices(bestChoices, runningSites);
      bestOverall = chooseBestAdaptation(bestChoices);
      //output bests, then all.
      new PlannerUtils(bestChoices, manager).printLatexDocument(orginal, bestOverall, false);
     /* if(!partialAds.isEmpty())
        new PlannerUtils(partialAds, manager).printLatexDocument(orginal, bestPartial, true);
      if(!localAds.isEmpty())
        new PlannerUtils(localAds, manager).printLatexDocument(orginal, bestLocal, true);*/
      System.out.println("sucessfully chose best adaptation, printing out latex");

    }
    catch(Exception e)
    {
      System.out.println(e.getMessage());
      e.printStackTrace();
    }
    return bestOverall;
  }

  private void doOrginalAssessment(Adaptation orginal, SensorNetworkQueryPlan oldQEP) 
  throws 
  IOException, OptimizationException, 
  SchemaMetadataException, TypeMappingException, 
  CodeGenerationException
  {
    orginal.setNewQep(oldQEP);
    assessor.assessChoice(orginal, runningSites, true);
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
