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
  private Successor currentBestSuccessor = null;
  private TABUList TABUList;
  private NeighbourhoodGenerator generator;
  private TABUSearchUtils Utils;
  private HashMap<String, RunTimeSite> initalSitesEnergy;
	private int numberOfIterationsWithoutImprovement = 2;
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
    TABUList.addToTABUList(InitialSuccessor, 0, true);
    ArrayList<Successor> initialList = new ArrayList<Successor>();
    initialList.add(InitialSuccessor);
    currentPath = new SuccessorPath(initialList);
    bestPath = new SuccessorPath(initialList);
    currentBestSuccessor = InitialSuccessor;
    try{
    int iteration = 0;
    int iterationsFailedAtInitial = 0;
    
    while(!StoppingCriteria.satisifiesStoppingCriteria(iteration, iterationsFailedAtInitial))
    {
      ArrayList<Successor> neighbourHood = 
        generator.generateNeighbourHood(currentBestSuccessor, currentPath.successorLength() -1, iteration);
      
      Successor bestNeighbourHoodSuccessor = locateBestSuccessor(neighbourHood, iteration);
      if(bestNeighbourHoodSuccessor != null)
      {
        iterationsFailedAtInitial = 0;
        checkNewSuccessor(bestNeighbourHoodSuccessor, iteration, currentPath, neighbourHood);
      }
      else
      {
        if(currentPath.successorLength() -1 == 0)
        {
          iterationsFailedAtInitial ++;
        }
        possibleDiversitySetoff(currentPath, iteration, neighbourHood);
      }
      Utils.outputTABUList(iteration, TABUList);
      iteration++;
    }
    Utils.close();
    return bestPath;
    }
    catch(Exception e)
    {
      e.printStackTrace();
      return null;
    }
  }

  private void possibleDiversitySetoff(SuccessorPath currentPath, int iteration,
                                       ArrayList<Successor> neighbourHood)
  throws IOException, OptimizationException, SchemaMetadataException, TypeMappingException
  {
    currentNumberOfIterationsWithoutImprovement++;
    Utils.writeNoSuccessor(iteration);
    if(currentNumberOfIterationsWithoutImprovement >= numberOfIterationsWithoutImprovement)
    {
      if(currentPath.overallAgendaLifetime() > bestPath.overallAgendaLifetime())
      {
        bestPath.updateList(currentPath.getSuccessorList());
      }
        currentBestSuccessor = 
          TABUList.engageDiversificationTechnique(neighbourHood, currentBestSuccessor, 
                                                  currentPath, iteration, Utils);
      //reset counter
      currentNumberOfIterationsWithoutImprovement = 0;
    }
  }

  private void checkNewSuccessor(Successor bestNeighbourHoodSuccessor,
                                 int iteration, SuccessorPath currentPath, 
                                 ArrayList<Successor> neighbourHood) 
  throws SchemaMetadataException, TypeMappingException, IOException, OptimizationException, 
  CodeGenerationException
  {
    try{
    if(fitness(bestNeighbourHoodSuccessor, currentBestSuccessor, iteration) > currentBestSuccessor.getLifetimeInAgendas())
    {
      Utils.writeNewSuccessor(bestNeighbourHoodSuccessor, iteration, currentBestSuccessor, currentPath);
      currentBestSuccessor = bestNeighbourHoodSuccessor;
      TABUList.addToTABUList(bestNeighbourHoodSuccessor, currentPath.successorLength() -1, false);
      currentPath.add(bestNeighbourHoodSuccessor);
      TABUList.addAllPathIntoTABUList(currentPath, currentPath.successorLength() -1, this.InitialSuccessor);
      
      if(currentPath.overallAgendaLifetime() > bestPath.overallAgendaLifetime())
        bestPath.updateList(currentPath.getSuccessorList());
      currentNumberOfIterationsWithoutImprovement = 0;
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
        currentBestSuccessor = 
           TABUList.engageDiversificationTechnique(neighbourHood, currentBestSuccessor, 
                                                  currentPath, iteration, Utils);
        //reset counter
        currentNumberOfIterationsWithoutImprovement = 0;
      }
    }
    }catch(Exception e)
    {
      e.printStackTrace();
    }
    
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
    ChoiceAssessor assessAdaptation = new ChoiceAssessor(_metadata, _metaManager, assessorFolder, true);
    System.out.println("assessing successor " + successor.toString());
    try
    {
      System.out.println("W");
      assessAdaptation.assessChoice(adapt, successor.getTheRunTimeSites(), false);
      System.out.println("E");
      successor.substractAdaptationCostOffRunTimeSites(adapt);
      System.out.println("WE");
      return successor.getLifetimeInAgendas();
    }
    catch(Exception e)
    {
      e.printStackTrace();
      return 0;
    }
  }

  /**
   * locates the best successor out of the neighbourhood
   * @param neighbourHood
   * @return
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws CodeGenerationException 
   * @throws OptimizationException 
   * @throws IOException 
   */
  private Successor locateBestSuccessor(ArrayList<Successor> neighbourHood, int iteration) 
  throws SchemaMetadataException, TypeMappingException,
  IOException, OptimizationException, CodeGenerationException
  {
    Iterator<Successor> neighbourHoodIterator = neighbourHood.iterator();
    int currentBestLifetime = Integer.MIN_VALUE;
    Successor bestSuccessor = null;
    while(neighbourHoodIterator.hasNext())
    {
      Successor successor = neighbourHoodIterator.next();
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
