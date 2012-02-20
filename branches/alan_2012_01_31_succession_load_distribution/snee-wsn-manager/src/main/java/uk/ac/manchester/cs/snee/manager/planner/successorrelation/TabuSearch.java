package uk.ac.manchester.cs.snee.manager.planner.successorrelation;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.logging.Logger;

import com.rits.cloning.Cloner;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.AutonomicManagerComponent;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.common.StrategyAbstract;
import uk.ac.manchester.cs.snee.manager.planner.ChoiceAssessor;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;

public class TabuSearch extends AutonomicManagerComponent
{
  private static final long serialVersionUID = -7392168097799519214L;
  private ArrayList<SensorNetworkQueryPlan> alternativePlans;
  private SuccessorPath bestPath = null;
  private Successor InitialSuccessor= null;
  private HashMap<Integer, ArrayList<Successor>> DiversificationTechniqueTABUExtra;
  private HashMap<String, RunTimeSite> initalSitesEnergy;
  private int TABUTenure = 5;
  private int AspirationPlusBounds = 30;
	private int numberOfRandomTimes = 4;
	private int numberOfIterationsWithoutImprovement = 2;
	private int currentNumberOfIterationsWithoutImprovement = 0;
	private SourceMetadataAbstract _metadata;
	private MetadataManager _metaManager;
	private File TABUOutputFolder = null;
	private String sep = System.getProperty("file.separator");
	private BufferedWriter out = null;
	
  /**
   * constructor
   * @param alternativePlans
   * @param runningSites
   * @throws IOException 
   */
  public TabuSearch(AutonomicManagerImpl autonomicManager, 
                    ArrayList<SensorNetworkQueryPlan> alternativePlans, 
                    HashMap<String, RunTimeSite> runningSites, SourceMetadataAbstract _metadata,
                    MetadataManager _metaManager, File outputFolder) 
  throws IOException
  {
    this.manager = autonomicManager;
	  this.alternativePlans = alternativePlans;
	  this.initalSitesEnergy = runningSites;
	  this._metadata = _metadata;
	  this._metaManager = _metaManager; 
	  this.TABUOutputFolder = outputFolder;
	  this.DiversificationTechniqueTABUExtra = new HashMap<Integer, ArrayList<Successor>>();
	  this.out = new BufferedWriter(new FileWriter(TABUOutputFolder + sep + "log"));
	  
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
   */
  public SuccessorPath findSuccessorsPath(SensorNetworkQueryPlan initialPoint) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException,
  IOException, CodeGenerationException
  {
    SuccessorPath currentPath = null;
    initalSitesEnergy = this.updateRunningSites(initalSitesEnergy, initialPoint);
    InitialSuccessor = new Successor(initialPoint, 0, this.initalSitesEnergy, this.initalSitesEnergy);
    ArrayList<Successor> initialList = new ArrayList<Successor>();
    initialList.add(InitialSuccessor);
    currentPath = new SuccessorPath(initialList);
    bestPath = new SuccessorPath(initialList);
    Successor currentBestSuccessor = InitialSuccessor;
    int iteration = 0;
    out.write("iteration \t nextSuccessorName \t nextSuccessorLifetime \t nextSuccessorSwitchPoint \n");
    out.flush();
    
    while(!StoppingCriteria.satisifiesStoppingCriteria(iteration))
    {
      ArrayList<Successor> neighbourHood = generateNeighbourHood(currentBestSuccessor.getCopyOfRunTimeSites(),
                                                                 currentBestSuccessor, currentPath);
      Successor bestNeighbourHoodSuccessor = locateBestSuccessor(neighbourHood, currentBestSuccessor,
                                                                 iteration);
      if(bestNeighbourHoodSuccessor != null)
      {
        if(fitness(bestNeighbourHoodSuccessor, currentBestSuccessor, iteration) > currentBestSuccessor.getLifetimeInAgendas())
        {
          currentBestSuccessor = bestNeighbourHoodSuccessor;
          addToTABUList(bestNeighbourHoodSuccessor, currentPath);
          
          currentPath.add(bestNeighbourHoodSuccessor);
          if(currentPath.overallAgendaLifetime() > bestPath.overallAgendaLifetime())
            bestPath = currentPath;
          out.write(iteration + " : " + bestNeighbourHoodSuccessor.toString() + " : " + bestNeighbourHoodSuccessor.getLifetimeInAgendas() + " : " + bestNeighbourHoodSuccessor.getAgendaCount());
        }
        else
        {
          out.write(" : BEST OPTION FAILED \n");
          currentNumberOfIterationsWithoutImprovement  ++;
          if(currentNumberOfIterationsWithoutImprovement == numberOfIterationsWithoutImprovement)
          {
            this.engageDiversificationTechnique(neighbourHood, currentBestSuccessor, currentPath);
            out.write(" : ENGADED DIVERSIFICATION TECHNIQUE \n");
            currentNumberOfIterationsWithoutImprovement = 0;
          }
          
        }
        out.flush();
      }
      else
      {
        out.flush();
      }
      iteration++;
    }
    out.close();
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
    File assessorFolder = new File(this.TABUOutputFolder.toString() + sep + iteration + ":Successor");
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
    assessAdaptation.assessChoice(adapt, successor.getTheRunTimeSites(), false);
    successor.substractAdaptationCostOffRunTimeSites(adapt);
    return successor.getLifetimeInAgendas();
  }

  /**
   * adds the attributes of the successor into the TABU list.
   * currently just adds the successor into the TABU list.
   * also removes old TABU members based off the TABUTenure
   * @param successor
   * @param currentPath 
   */
  private void addToTABUList(Successor successor, SuccessorPath currentPath)
  {
    int position = 0;
    ArrayList<Successor> DiversificationTABUList = null;
    //get size of current path
    if(currentPath != null)
      position = currentPath.getSuccessorList().size();
    //get correct tabuList
    if(DiversificationTechniqueTABUExtra.get(position) == null)
    {
      DiversificationTABUList = new ArrayList<Successor>();
    }
    else
    {
      DiversificationTABUList = DiversificationTechniqueTABUExtra.get(position); 
    }
    //add to list
    DiversificationTABUList.add(successor);
    
    //remove any off TABUTenure
    while(DiversificationTABUList.size() > TABUTenure)
    {
      DiversificationTABUList.remove(0);
    }
    //restore
    DiversificationTechniqueTABUExtra.put(position, DiversificationTABUList);
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
      int successorLifetimeTime = fitness(successor, currentBestSuccessor, iteration);
      if(currentBestLifetime < successorLifetimeTime)
      {
        currentBestLifetime = successorLifetimeTime;
        bestSuccessor = successor;
      }
    }
    return bestSuccessor;
  }

  /**
   * checks if the new successor meets any of the criteria for TABU. 
   * (currently this means if the successor is in the TABU list.
   * but could easily be attributed).
   * @param successor
   * @param currentPath 
   * @return
   */
  private boolean meetsTABUCriteria(Successor successor, SuccessorPath currentPath)
  {
    //locates the diversification TABU list for long term memory
    ArrayList<Successor> DiversificationTABUList = new ArrayList<Successor>();
    if(currentPath != null)
    {
      if(DiversificationTechniqueTABUExtra.get(currentPath.successorLength()) != null)
      {
        DiversificationTABUList = 
          DiversificationTechniqueTABUExtra.get(currentPath.successorLength());
      }   
    }
    if(DiversificationTABUList.contains(successor))
      return true;
    else
      return false;
  }

  /**
   * generates the neighbourhood to be assessed. 
   * Uses aspiration plus to reduce size of candidates in the neighbourhood.
   * @param currentBestSuccessor 
   * @param currentPath 
   * @return
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws IOException 
   */
  private ArrayList<Successor> generateNeighbourHood(HashMap<String, RunTimeSite> runTimeSites, 
                                                     Successor currentBestSuccessor,
                                                     SuccessorPath currentPath) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException, IOException
  {
    ArrayList<Successor> neighbourHood = new ArrayList<Successor>();
    ArrayList<Successor> alternativePlansTemp = new ArrayList<Successor>();
    //generate successors
    Iterator<SensorNetworkQueryPlan> altPlanIterator = this.alternativePlans.iterator();
    while(altPlanIterator.hasNext())
    {
      SensorNetworkQueryPlan altPlan = altPlanIterator.next();
      HashMap<String, RunTimeSite> altPlanRunTimeSites = updateRunningSites(runTimeSites, altPlan);
     
      
      //determine best point to switch
      Successor maxLifetimeSucessor = new Successor(altPlan, 0, altPlanRunTimeSites, runTimeSites);
      int previousBestLifetime = currentBestSuccessor.getLifetimeInAgendas();
      int lifetime = maxLifetimeSucessor.getLifetimeInAgendas();
      int agendaCount = determineBestAgendaSwitch(altPlan, currentBestSuccessor, 
                                                  lifetime, altPlanRunTimeSites, runTimeSites);
      Successor Sucessor = new Successor(altPlan, agendaCount, altPlanRunTimeSites, runTimeSites);
      alternativePlansTemp.add(Sucessor);
      
      //do random generation 
      Random waitingAgendasGenerator = new Random();
      for(int option = 0; option < this.numberOfRandomTimes; option++)
      {
        int nextAgendaCount = waitingAgendasGenerator.nextInt(previousBestLifetime);
        Sucessor = new Successor(altPlan, nextAgendaCount, altPlanRunTimeSites, runTimeSites);
        alternativePlansTemp.add(Sucessor);
      }
    }
    
    //search neighbourHood for candidates
    Random successorChooser = new Random();
    while(neighbourHood.size() <= AspirationPlusBounds && alternativePlansTemp.size() > 0)
    {
      int overallPosition = successorChooser.nextInt(alternativePlansTemp.size());
      
      Successor successor = alternativePlansTemp.get(overallPosition);
      if(this.meetsTABUCriteria(successor, currentPath))
      {
        if(this.passesAspirationCriteria(successor, currentBestSuccessor))
          neighbourHood.add(successor);
        alternativePlansTemp.remove(overallPosition);
      }
      else
      {
        neighbourHood.add(successor);
        alternativePlansTemp.remove(overallPosition);
      }
    }
    
    if(neighbourHood.size() == 0)
    {
      engageDiversificationTechnique(neighbourHood, currentBestSuccessor, currentPath);
      return generateNeighbourHood(runTimeSites, currentBestSuccessor, currentPath);
    }
    return neighbourHood;
  }

  
  /**
   * used to restart the search 
   * @param neighbourHood
   * @param currentBestSuccessor
   * @param current path
   * @throws IOException 
   */
  private void engageDiversificationTechnique(ArrayList<Successor> neighbourHood, 
                                              Successor currentBestSuccessor,
                                              SuccessorPath currentPath) 
  throws IOException
  { 
    out.write("engaded in Diversification Strategy \n");
    out.flush();
    if(currentPath.overallAgendaLifetime() > bestPath.overallAgendaLifetime())
    {
      bestPath.updateList(currentPath.getSuccessorList());
    }
    
    //update TABUList
    int length = bestPath.successorLength();
    int positionToMoveTo = length -1;
    for(int position = currentPath.successorLength(); position > positionToMoveTo; position--)
    {
      ArrayList<Successor> DiverseTABUList = DiversificationTechniqueTABUExtra.get(position);
      if(DiverseTABUList != null)
      {
        DiverseTABUList.add(currentPath.getSuccessorList().get(position));
        DiversificationTechniqueTABUExtra.put(position, DiverseTABUList);
      }
      else
      {
        DiverseTABUList = new  ArrayList<Successor>();
        DiverseTABUList.add(currentPath.getSuccessorList().get(position));
        DiversificationTechniqueTABUExtra.put(position, DiverseTABUList);
      }
      currentPath.removeSuccessor(position);
      
      //remove all tabuList after the next position
      Iterator<Integer> keyIterator = DiversificationTechniqueTABUExtra.keySet().iterator();
      while(keyIterator.hasNext())
      {
        Integer key = keyIterator.next();
        if(key > positionToMoveTo +1)
          DiversificationTechniqueTABUExtra.remove(key);
      }
    }
  }

  /**
   * checks the successor to see if it gives a large benefit (at which point will allow out of 
   * the TABU list)
   * 
   * @param successor
   * @return
   */
  private boolean passesAspirationCriteria(Successor successor, Successor bestCurrentSuccessor)
  {
    if(successor.getLifetimeInAgendas() > bestCurrentSuccessor.getLifetimeInAgendas())
      return true;
    else
      return false;
  }

  /**
   * by comparing lifetimes between the current plan and alternative plan, 
   * generates a estimate for the best agenda time to switch.
   * @param altPlan
   * @param currentBestSuccessor
   * @param lifetimeInAgendas 
   * @param altPlanRunTimeSites 
   * @return
   */
  private int determineBestAgendaSwitch(SensorNetworkQueryPlan altPlan,
                                        Successor currentBestSuccessor, int lifetimeInAgendas, 
                                        HashMap<String, RunTimeSite> altPlanRunTimeSites,
                                        HashMap<String, RunTimeSite> originalRunTimeSites)
  {
    int maxAgenda = currentBestSuccessor.getLifetimeInAgendas();
    int minAgenda = 1;
    Cloner cloner = new Cloner();
    cloner.dontClone(Logger.class);
    
    do
    {
      int middle = (minAgenda + maxAgenda) /2;
      int left = middle - 1;
      
      HashMap<String, RunTimeSite> runTimeSites = cloner.deepClone(altPlanRunTimeSites);
      Successor middleSuccessor = new Successor(altPlan, middle, runTimeSites, originalRunTimeSites);
      runTimeSites = cloner.deepClone(altPlanRunTimeSites);
      Successor leftSuccessor = new Successor(altPlan, left, runTimeSites, originalRunTimeSites);
      
      int middleLifetime = middleSuccessor.getLifetimeInAgendas();
      int leftLifetime = leftSuccessor.getLifetimeInAgendas();
      
      if(middleLifetime > leftLifetime)
      {
        minAgenda = left;
      }
      else
        maxAgenda = left;
      
    }while (minAgenda < maxAgenda);
    return minAgenda;
    
    /*
    int bestAgendaWaitTime = 0;
    int bestAgendasLifetime = Integer.MIN_VALUE;
    
    for(int agendaTestPoint = 0; agendaTestPoint < lifetimeInAgendas; agendaTestPoint ++)
    {
      Cloner cloner = new Cloner();
      cloner.dontClone(Logger.class);
      HashMap<String, RunTimeSite> runTimeSites = cloner.deepClone(altPlanRunTimeSites);
      Successor successor = new Successor(altPlan, agendaTestPoint, runTimeSites, originalRunTimeSites);
      int agendasLifetime = successor.getLifetimeInAgendas();
      if(agendasLifetime > bestAgendasLifetime)
      {
        bestAgendaWaitTime = agendaTestPoint;
        bestAgendasLifetime = agendasLifetime;
      }
    }
    return bestAgendaWaitTime;*/
   // return 0;
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
