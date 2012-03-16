package uk.ac.manchester.cs.snee.manager.planner;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.WhenSchedulerException;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.AdaptationCollection;
import uk.ac.manchester.cs.snee.manager.common.AutonomicManagerComponent;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.failednode.FailedNodeStrategyLocal;
import uk.ac.manchester.cs.snee.manager.failednode.cluster.LogicalOverlayNetwork;
import uk.ac.manchester.cs.snee.manager.planner.successorrelation.SuccessorRelation;
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
  private MetadataManager _metadataManager;
  
  public Planner(AutonomicManagerImpl autonomicManager, SourceMetadataAbstract _metadata, MetadataManager _metadataManager)   
  {
    manager = autonomicManager;
    assessor = new ChoiceAssessor(_metadata, _metadataManager, plannerFolder);
    this._metadata = _metadata;
    this._metadataManager = _metadataManager;
    runningSites = manager.getRunningSites();
  }
  
  public Planner(AutonomicManagerImpl autonomicManager, SourceMetadataAbstract _metadata, MetadataManager _metadataManager,
                 HashMap<String, RunTimeSite> runningSites, File plannerFolder)   
  {
    manager = autonomicManager;
    this.plannerFolder = plannerFolder;
    assessor = new ChoiceAssessor(_metadata, _metadataManager, plannerFolder);
    this.runningSites = runningSites;
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
      orginal = choices.getOriginal(); 
      doOrginalAssessment(orginal, orginal.getOldQep());
      
      //find the best of each framework
      if(!partialAds.isEmpty())
      {
        assessor.assessChoices(partialAds, runningSites);
        bestPartial = chooseBestAdaptation(partialAds, ChoiceAssessorPreferenceEnum.Partial.toString());
      }
      
      if(!localAds.isEmpty())
      {
        assessor.assessChoices(localAds, runningSites);
        
        bestLocal = chooseBestAdaptation(localAds,  ChoiceAssessorPreferenceEnum.Local.toString());
      }
      //set up new collection for the assessor
      List<Adaptation> bestChoices = new ArrayList<Adaptation>();
      if(bestLocal != null)
        bestChoices.add(bestLocal);
      if(bestPartial != null)
        bestChoices.add(bestPartial);
      if(choices.getGlobalAdaptation() != null)
        bestChoices.add(choices.getGlobalAdaptation());
      //assess to find best overall adaptation
      assessor.assessChoices(bestChoices, runningSites);
      String choicePreference = SNEEProperties.getSetting(SNEEPropertyNames.CHOICE_ASSESSOR_PREFERENCE);    
      bestOverall = chooseBestAdaptation(bestChoices, choicePreference);
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

  /**
   * assesses the old qep.
   * @param orginal
   * @param oldQEP
   * @throws IOException
   * @throws OptimizationException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws CodeGenerationException
   */
  private void doOrginalAssessment(Adaptation orginal, SensorNetworkQueryPlan oldQEP) 
  throws 
  IOException, OptimizationException, 
  SchemaMetadataException, TypeMappingException, 
  CodeGenerationException
  {
    orginal.setNewQep(oldQEP);
    assessor.assessChoice(orginal, runningSites, true);
  }

  /**
   * chooses the best adaptation in accordance with the choice preference
   * @param choices
   * @param choicePreference
   * @return
   * @throws SNEEConfigurationException
   */
  private Adaptation chooseBestAdaptation(List<Adaptation> choices, String choicePreference) throws SNEEConfigurationException
  {
    Adaptation finalChoice = null;
    Double cost = Double.MIN_VALUE;
    Iterator<Adaptation> choiceIterator = choices.iterator();
    //calculate each cost, and compares it with the best so far, if the same, store it  
    if(choicePreference.equals(ChoiceAssessorPreferenceEnum.Best.toString()))
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
    else if(choicePreference.equals(ChoiceAssessorPreferenceEnum.Local.toString()))
    {
      while(choiceIterator.hasNext())
      {
        Adaptation choice = choiceIterator.next();
        if(choice.getStrategyId().toString().contains("Local"))
          return choice;
      }
    }
    else if(choicePreference.equals(ChoiceAssessorPreferenceEnum.Partial.toString()))
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
    else if(choicePreference.equals(ChoiceAssessorPreferenceEnum.Global.toString()))
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

  /**
   * changes the folder to which outputs are stored
   * @param outputFolder
   * @throws IOException
   */
  public void updateStorageLocation(File outputFolder) throws IOException
  {
    plannerFolder = new File(outputFolder.toString() + sep + "Planner");
    setupFolders();
    this.assessor.updateStorageLocation(plannerFolder);
  }

  /**
   * assesses the cost gained by the OTA procedure to move the original qep over
   * @param output
   * @param orgianlOTAProgramCost
   * @param runningSites
   * @param reset
   * @param logicalOverlayNetwork
   * @throws IOException
   * @throws OptimizationException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws CodeGenerationException
   * @throws SNEEConfigurationException
   */
  public void assessOTACosts(File output, Adaptation orgianlOTAProgramCost,
      HashMap<String, RunTimeSite> runningSites, boolean reset, LogicalOverlayNetwork logicalOverlayNetwork) 
  throws IOException, OptimizationException, SchemaMetadataException, 
  TypeMappingException, CodeGenerationException, SNEEConfigurationException
  {
    this.assessor.updateStorageLocation(output);
    if(logicalOverlayNetwork == null)
      this.assessor.assessChoice(orgianlOTAProgramCost, runningSites, reset);
    else
      this.assessor.assessChoice(orgianlOTAProgramCost, runningSites, reset, logicalOverlayNetwork);
     
    this.assessor.updateStorageLocation(plannerFolder);
    new PlannerUtils(orgianlOTAProgramCost, manager, output, orgianlOTAProgramCost).writeObjectsToFile(); 
    new ChoiceAssessorUtils(runningSites, orgianlOTAProgramCost.getNewQep().getRT())
    .exportRTWithEnergies(output.toString()+ sep + "energies" , "");
  }

  /**
   * assesses the cost gained by the OTA procedure to move the orginal qep over with a specfic overlay
   * @param outputFolder
   * @param overlayOTAProgramCost
   * @param current
   * @param failedNodeStrategyLocal
   * @throws OptimizationException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws IOException
   * @throws CodeGenerationException
   * @throws SNEEConfigurationException
   */
  public void assessOverlayCosts(File outputFolder, Adaptation overlayOTAProgramCost, 
                                 LogicalOverlayNetwork current,
                                 FailedNodeStrategyLocal failedNodeStrategyLocal) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException,
  IOException, CodeGenerationException, SNEEConfigurationException
  {
    this.assessor.updateStorageLocation(outputFolder);
    this.assessor.assessOverlayChoice(overlayOTAProgramCost, runningSites, current, failedNodeStrategyLocal);
    this.assessor.updateStorageLocation(plannerFolder);
  }
  
  /**
   * starts the successor relation resulting in a path of successor plans to extend the lifetime
   * @param qep
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   * @throws TypeMappingException 
   * @throws WhenSchedulerException 
   * @throws OptimizationException 
   * @throws SchemaMetadataException 
   * @throws SNEEException 
   * @throws CodeGenerationException 
   * @throws IOException 
   */
  public void startSuccessorRelation(SensorNetworkQueryPlan qep) 
  throws NumberFormatException, SNEEConfigurationException,
  SNEEException, SchemaMetadataException, OptimizationException, 
  WhenSchedulerException, TypeMappingException, IOException, CodeGenerationException
  {
    SuccessorRelation successorRelation = new SuccessorRelation(plannerFolder, runningSites, _metadataManager, _metadata, manager);
    successorRelation.executeSuccessorRelation(qep);
  }
  
}
