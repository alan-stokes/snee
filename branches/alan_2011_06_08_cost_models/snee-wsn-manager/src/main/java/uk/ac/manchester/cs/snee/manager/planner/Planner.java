package uk.ac.manchester.cs.snee.manager.planner;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.AdaptationCollection;
import uk.ac.manchester.cs.snee.manager.common.AutonomicManagerComponent;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.common.StrategyIDEnum;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;

public class Planner extends AutonomicManagerComponent
{

  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -8013340359332149778L;
  private ChoiceAssessor assessor;
  private HashMap<String, RunTimeSite> runningSites;
  private File plannerFolder = null;
  
  public Planner(AutonomicManagerImpl autonomicManager, SourceMetadataAbstract _metadata, MetadataManager _metadataManager)   
  {
    manager = autonomicManager;
    assessor = new ChoiceAssessor(_metadata, _metadataManager, plannerFolder);
    runningSites = manager.getRunningSites();
  }
  
/**
 * sets up folder to store output from the planner and its sub functions
 * @param outputFolder
 */
  private void setupFolders()
  {
    if(plannerFolder.exists())
    {
      this.deleteFolder(plannerFolder);
      plannerFolder.mkdir();
    }
    else
    {
      plannerFolder.mkdir();
    }
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
      orginal = new Adaptation(choices.getGlobalAdaptation().getOldQep(), StrategyIDEnum.Orginal, 1);
      doOrginalAssessment(orginal, choices.getGlobalAdaptation().getOldQep());
      
      //find the best of each framework
      if(!partialAds.isEmpty())
      {
        assessor.assessChoices(partialAds, runningSites);
        bestPartial = chooseBestAdaptation(partialAds, false);
      }
      
      if(!localAds.isEmpty())
      {
        assessor.assessChoices(localAds, runningSites);
        bestLocal = chooseBestAdaptation(localAds, false);
      }
      //set up new collection for the assessor
      List<Adaptation> bestChoices = new ArrayList<Adaptation>();
      if(bestLocal != null)
        bestChoices.add(bestLocal);
      if(bestPartial != null)
        bestChoices.add(bestPartial);
      bestChoices.add(choices.getGlobalAdaptation());
      //assess to find best overall adaptation
      assessor.assessChoices(bestChoices, runningSites);
      bestOverall = chooseBestAdaptation(bestChoices, true);
      //output bests, then all.
      new PlannerUtils(bestChoices, manager, plannerFolder, orginal).writeObjectsToFile();
     /* if(!partialAds.isEmpty())
        new PlannerUtils(partialAds, manager).printLatexDocument(orginal, bestPartial, true);
      if(!localAds.isEmpty())
        new PlannerUtils(localAds, manager).printLatexDocument(orginal, bestLocal, true);*/

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

  private Adaptation chooseBestAdaptation(List<Adaptation> choices, boolean bestOverall) throws SNEEConfigurationException
  {
    Adaptation finalChoice = null;
    Double cost = Double.MIN_VALUE;
    Iterator<Adaptation> choiceIterator = choices.iterator();
    //calculate each cost, and compares it with the best so far, if the same, store it 
    String choicePreference = SNEEProperties.getSetting(SNEEPropertyNames.CHOICE_ASSESSOR_PREFERENCE);       
    
    if(!bestOverall || choicePreference.equals(ChoiceAssessorPreferenceEnum.Best.toString()))
    {
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
    else if(!bestOverall || choicePreference.equals(ChoiceAssessorPreferenceEnum.Local.toString()))
    {
      while(choiceIterator.hasNext())
      {
        Adaptation choice = choiceIterator.next();
        if(choice.getStrategyId().toString().contains("Local"))
          return choice;
      }
    }
    else if(!bestOverall || choicePreference.equals(ChoiceAssessorPreferenceEnum.Partial.toString()))
    {
      while(choiceIterator.hasNext())
      {
        Adaptation choice = choiceIterator.next();
        if(choice.getStrategyId().toString().contains("Partial"))
        {
          Double choiceCost = choice.getLifetimeEstimate();
          if(choiceCost > cost)
          {
            finalChoice = choice;
            cost = choiceCost;
          }
        }
      }
      return finalChoice;
    }
    else if(!bestOverall || choicePreference.equals(ChoiceAssessorPreferenceEnum.Global.toString()))
    {
      while(choiceIterator.hasNext())
      {
        Adaptation choice = choiceIterator.next();
        if(choice.getStrategyId().toString().contains("Global"))
          return choice;
      }
    }
    else
    {
      throw new SNEEConfigurationException("no recgonised chioce was given. will return null");
    }
    return null;
  }

  public void updateStorageLocation(File outputFolder) throws IOException
  {
    plannerFolder = new File(outputFolder.toString() + sep + "Planner");
    setupFolders();
    this.assessor.updateStorageLocation(plannerFolder);
  }

  public void assessOTACosts(File output, Adaptation orgianlOTAProgramCost,
      HashMap<String, RunTimeSite> runningSites, boolean reset) 
  throws IOException, OptimizationException, SchemaMetadataException, 
  TypeMappingException, CodeGenerationException
  {
    this.assessor.updateStorageLocation(output);
    this.assessor.assessChoice(orgianlOTAProgramCost, runningSites, reset);
    this.assessor.updateStorageLocation(plannerFolder);
    new PlannerUtils(orgianlOTAProgramCost, manager, output, orgianlOTAProgramCost).writeObjectsToFile(); 
  }
  
}
