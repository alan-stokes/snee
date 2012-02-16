package uk.ac.manchester.cs.snee.manager.planner.successorrelation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.logging.Logger;

import com.rits.cloning.Cloner;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.common.StrategyAbstract;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class TabuSearch 
{
  private ArrayList<SensorNetworkQueryPlan> alternativePlans;
  private ArrayList<Successor> successorPath = new ArrayList<Successor>();
  private Successor InitialSuccessor= null;
  private ArrayList<Successor> TABUList = new ArrayList<Successor>();
  private HashMap<String, RunTimeSite> initalSitesEnergy;
  private int TABUTenure = 5;
  private int AspirationPlusBounds = 10;
	private int numberOfRandomTimes = 4;
  
  /**
   * constructor
   * @param alternativePlans
   * @param runningSites
   */
  public TabuSearch(ArrayList<SensorNetworkQueryPlan> alternativePlans, 
                              HashMap<String, RunTimeSite> runningSites)
  {
	  this.alternativePlans = alternativePlans;
	  this.initalSitesEnergy = runningSites;
  }
  
  /**
   * searches the search space looking for the best path from initial to final plan
   * @param initialPoint
   * @return
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  public ArrayList<Successor> findSuccessorsPath(SensorNetworkQueryPlan initialPoint) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    this.updateRunningSites(initalSitesEnergy, initialPoint);
    InitialSuccessor = new Successor(initialPoint, 0, this.initalSitesEnergy, this.initalSitesEnergy);
    Successor currentBestSuccessor = InitialSuccessor;
    int iteration = 0;
    while(!StoppingCriteria.satisifiesStoppingCriteria(iteration))
    {
      ArrayList<Successor> neighbourHood = generateNeighbourHood(currentBestSuccessor.getCopyOfRunTimeSites(),
                                                                 currentBestSuccessor);
      Successor bestNeighbourHoodSuccessor = locateBestSuccessor(neighbourHood, currentBestSuccessor);
      if(bestNeighbourHoodSuccessor != null)
      {
        if(fitness(bestNeighbourHoodSuccessor, currentBestSuccessor) > currentBestSuccessor.getLifetimeInAgendas())
        {
          currentBestSuccessor = bestNeighbourHoodSuccessor;
          addToTABUList(bestNeighbourHoodSuccessor);
          successorPath.add(bestNeighbourHoodSuccessor);
        }
      }
      iteration++;
    }
    return successorPath;
  }

  /**
   * determines the lifetime of the successor after adapting. 
   * @param successor
   * @return lifetime of the successor
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   */
  private int fitness(Successor successor, Successor currentPosition) 
  throws SchemaMetadataException, TypeMappingException
  {
    Adaptation adapt = StrategyAbstract.generateAdaptationObject(currentPosition.getQep(), successor.getQep());
    successor.substractAdaptationCostOffRunTImeSites(adapt);
    return successor.getLifetimeInAgendas();
  }

  /**
   * adds the attributes of the successor into the TABU list.
   * currently just adds the successor into the TABU list.
   * also removes old TABU members based off the TABUTenure
   * @param successor
   */
  private void addToTABUList(Successor successor)
  {
    TABUList.add(successor);
    while(TABUList.size() > TABUTenure)
    {
      TABUList.remove(0);
    }
  }

  /**
   * locates the best successor out of the neighbourhood
   * @param neighbourHood
   * @param currentBestSuccessor 
   * @return
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   */
  private Successor locateBestSuccessor(ArrayList<Successor> neighbourHood, 
                                        Successor currentBestSuccessor) 
  throws SchemaMetadataException, TypeMappingException
  {
    Iterator<Successor> neighbourHoodIterator = neighbourHood.iterator();
    int currentBestLifetime = currentBestSuccessor.getLifetimeInAgendas();
    Successor bestSuccessor = null;
    while(neighbourHoodIterator.hasNext())
    {
      Successor successor = neighbourHoodIterator.next();
      int successorLifetimeTime = fitness(successor, currentBestSuccessor);
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
   * @return
   */
  private boolean meetsTABUCriteria(Successor successor)
  {
    if(TABUList.contains(successor))
      return true;
    else
      return false;
  }

  /**
   * generates the neighbourhood to be assessed. 
   * Uses aspiration plus to reduce size of candidates in the neighbourhood.
   * @param currentBestSuccessor 
   * @return
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  private ArrayList<Successor> generateNeighbourHood(HashMap<String, RunTimeSite> runTimeSites, 
                                                     Successor currentBestSuccessor) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException
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
      int lifetime = maxLifetimeSucessor.getLifetimeInAgendas();
      int agendaCount = determineBestAgendaSwitch(altPlan, currentBestSuccessor, 
                                                  lifetime, altPlanRunTimeSites, runTimeSites);
      Successor Sucessor = new Successor(altPlan, agendaCount, altPlanRunTimeSites, runTimeSites);
      alternativePlansTemp.add(Sucessor);
      
      //do random generation 
      Random waitingAgendasGenerator = new Random();
      for(int option = 0; option < this.numberOfRandomTimes; option++)
      {
        int nextAgendaCount = waitingAgendasGenerator.nextInt(lifetime);
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
      if(this.meetsTABUCriteria(successor))
        alternativePlansTemp.remove(overallPosition);
      else
      {
        neighbourHood.add(successor);
        alternativePlansTemp.remove(overallPosition);
      }
    }
    return neighbourHood;
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
    return bestAgendaWaitTime;
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
