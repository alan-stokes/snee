package uk.ac.manchester.cs.snee.manager.planner.successorrelation.tabu;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;

import com.rits.cloning.Cloner;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.WhenSchedulerException;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.AutonomicManagerComponent;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.common.StrategyAbstract;
import uk.ac.manchester.cs.snee.manager.planner.ChoiceAssessor;
import uk.ac.manchester.cs.snee.manager.planner.common.Successor;
import uk.ac.manchester.cs.snee.manager.planner.common.SuccessorPath;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;

public class TabuSearch extends AutonomicManagerComponent
{
  private static final long serialVersionUID = -7392168097799519214L;
  private SuccessorPath bestPath = null;
  private Successor InitialSuccessor= null;
  private TABUList TABUList;
  private NeighbourhoodGenerator generator;
  private TABUSearchUtils Utils;
  private HashMap<String, RunTimeSite> initalSitesEnergy;
	private int numberOfIterationsWithoutImprovement = 5;
	private int currentNumberOfIterationsWithoutImprovement = 0;
	private SourceMetadataAbstract _metadata;
	private MetadataManager _metaManager;
	private File TABUOutputFolder = null;
	
  /**
   * constructor
   * @param alternativePlans
   * @param runningSites
   * @throws IOException 
   */
  public TabuSearch(AutonomicManagerImpl autonomicManager, 
                    HashMap<String, RunTimeSite> runningSites, 
                    SourceMetadataAbstract _metadata,
                    MetadataManager _metaManager, File outputFolder) 
  throws IOException
  {
    this.manager = autonomicManager;
	  this.initalSitesEnergy = runningSites;
	  this._metadata = _metadata;
	  this._metaManager = _metaManager; 
	  this.TABUOutputFolder = outputFolder;
	  this.Utils = new TABUSearchUtils(TABUOutputFolder.toString());
	  this.TABUList = new TABUList();
	  this.generator = new NeighbourhoodGenerator(TABUList, autonomicManager, _metaManager,
	                                              runningSites,_metadata, outputFolder);
  }
  
  /**
   * searches the search space looking for the best path from initial to final plan
   * @param initialPoint
   * @return
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws CodeGenerationException 
   * @throws IOException 
   * @throws WhenSchedulerException 
   * @throws SNEEException 
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   */
  public SuccessorPath findSuccessorsPath(SensorNetworkQueryPlan initialPoint) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException,
  IOException, CodeGenerationException,
  NumberFormatException, SNEEConfigurationException,
  SNEEException, WhenSchedulerException
  {
    SuccessorPath currentPath = null;
    initalSitesEnergy = this.updateRunningSites(initalSitesEnergy, initialPoint);
    InitialSuccessor = new Successor(initialPoint, 0, this.initalSitesEnergy, 0);
    ArrayList<Successor> initialList = new ArrayList<Successor>();
    initialList.add(InitialSuccessor);
    currentPath = new SuccessorPath(initialList);
    bestPath = new SuccessorPath(initialList);
    Successor currentBestSuccessor = InitialSuccessor;
    int iteration = 0;
    
    while(!StoppingCriteria.satisifiesStoppingCriteria(iteration))
    {
      ArrayList<Successor> neighbourHood = 
        generator.generateNeighbourHood(currentBestSuccessor, currentPath.successorLength() -1, iteration);
      
      Successor bestNeighbourHoodSuccessor = locateBestSuccessor(neighbourHood, currentBestSuccessor,
                                                                 iteration);
      if(bestNeighbourHoodSuccessor != null)
      {
        if(fitness(bestNeighbourHoodSuccessor, currentBestSuccessor, iteration) > currentBestSuccessor.getLifetimeInAgendas())
        {
          Utils.writeNewSuccessor(bestNeighbourHoodSuccessor, iteration, currentBestSuccessor, currentPath);
          currentBestSuccessor = bestNeighbourHoodSuccessor;
          TABUList.addToTABUList(bestNeighbourHoodSuccessor, currentPath.successorLength() -1, false);
          currentPath.add(bestNeighbourHoodSuccessor);
          TABUList.addAllPathIntoTABUList(currentPath, currentPath.successorLength() -1, this.InitialSuccessor);
          
          if(currentPath.overallAgendaLifetime() > bestPath.overallAgendaLifetime())
            bestPath = currentPath;
        }
        else
        {
          Utils.writeFailedSuccessor(bestNeighbourHoodSuccessor, iteration,currentBestSuccessor , currentPath);
          TABUList.addToTABUList(bestNeighbourHoodSuccessor, currentPath.successorLength() -1, false);
          currentNumberOfIterationsWithoutImprovement++;
          if(currentNumberOfIterationsWithoutImprovement >= numberOfIterationsWithoutImprovement)
          {
            if(currentPath.overallAgendaLifetime() > bestPath.overallAgendaLifetime())
            {
              bestPath.updateList(currentPath.getSuccessorList());
            }
            int positionToMoveTo = currentPath.successorLength() -2;
            if(positionToMoveTo >= 0)
            {
              Utils.outputDiversification(iteration, positionToMoveTo);
              currentBestSuccessor = TABUList.engageDiversificationTechnique(neighbourHood, currentBestSuccessor, currentPath, iteration);
            }
            else
              Utils.outputNODiversification(iteration);
            //reset counter
            currentNumberOfIterationsWithoutImprovement = 0;
          }
        }
      }
      Utils.outputTABUList(iteration, TABUList);
      iteration++;
    }
    Utils.close();
    if(currentPath.overallAgendaLifetime() > bestPath.overallAgendaLifetime())
      bestPath = currentPath;
    return bestPath;
  }

  /**
   * determines the lifetime of the successor after adapting. 
   * @param successor
   * @return lifetime of the successor
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws CodeGenerationException 
   * @throws OptimizationException 
   * @throws IOException 
   */
  private int fitness(Successor successor, Successor currentPosition, int iteration) 
  throws SchemaMetadataException, TypeMappingException, IOException,
  OptimizationException, CodeGenerationException
  {
    Adaptation adapt = StrategyAbstract.generateAdaptationObject(currentPosition.getQep(), successor.getQep());
    File assessorFolder = new File(this.TABUOutputFolder.toString() + sep + "Successor");
    if(assessorFolder.exists())
    {
      manager.deleteFileContents(assessorFolder);
      assessorFolder.mkdir();
    }
    else
    {
      assessorFolder.mkdir();
    }
    ChoiceAssessor assessAdaptation = new ChoiceAssessor(_metadata, _metaManager, assessorFolder);
    System.out.println("assessing successor " + successor.toString());
    try
    {
      assessAdaptation.assessChoice(adapt, successor.getTheRunTimeSites(), false);
    }
    catch(Exception e)
    {
      e.printStackTrace();
      return 0;
    }
    successor.substractAdaptationCostOffRunTimeSites(adapt);
    return successor.getLifetimeInAgendas();
  }

  /**
   * locates the best successor out of the neighbourhood
   * @param neighbourHood
   * @param currentBestSuccessor 
   * @return
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws CodeGenerationException 
   * @throws OptimizationException 
   * @throws IOException 
   */
  private Successor locateBestSuccessor(ArrayList<Successor> neighbourHood, 
                                        Successor currentBestSuccessor,
                                        int iteration) 
  throws SchemaMetadataException, TypeMappingException,
  IOException, OptimizationException, CodeGenerationException
  {
    Iterator<Successor> neighbourHoodIterator = neighbourHood.iterator();
    int currentBestLifetime = Integer.MIN_VALUE;
    Successor bestSuccessor = null;
    while(neighbourHoodIterator.hasNext())
    {
      Successor successor = neighbourHoodIterator.next();
      //TODO remove if need be (currently used to reduce neighbourhood size).
      //this.addToTABUList(successor);
      //int successorLifetimeTime = fitness(successor, currentBestSuccessor, iteration);
      int successorLifetimeTime = successor.getLifetimeInAgendas();
      if(currentBestLifetime < successorLifetimeTime)
      {
        currentBestLifetime = successorLifetimeTime;
        bestSuccessor = successor;
      }
    }
    return bestSuccessor;
  }

  /**
   * updates a copy of the runTimeSites with the plans QEP.
   * @param runTimeSites
   * @return
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  private HashMap<String, RunTimeSite> updateRunningSites(HashMap<String, RunTimeSite> sites,
                                                          SensorNetworkQueryPlan altPlan) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    Cloner cloner = new Cloner();
    cloner.dontClone(Logger.class);
    HashMap<String, RunTimeSite> runTimeSites = cloner.deepClone(sites);
    
    Iterator<Site> siteIter = altPlan.getIOT().getRT().siteIterator(TraversalOrder.POST_ORDER);
    while (siteIter.hasNext()) 
    {
      Site site = siteIter.next();
      double siteEnergyCons = altPlan.getAgendaIOT().getSiteEnergyConsumption(site); // J
      runTimeSites.get(site.getID()).setQepExecutionCost(siteEnergyCons);
    }
    return runTimeSites;
  }
     
}
