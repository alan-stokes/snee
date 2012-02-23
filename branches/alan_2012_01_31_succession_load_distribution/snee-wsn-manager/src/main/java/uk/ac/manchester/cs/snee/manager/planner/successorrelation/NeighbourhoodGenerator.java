package uk.ac.manchester.cs.snee.manager.planner.successorrelation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;

public class NeighbourhoodGenerator
{
  private ArrayList<SensorNetworkQueryPlan> alternativePlans;
  private TABUList tabuList;
  private int AspirationPlusBounds = 30;
  private int numberOfRandomTimes = 4;
  
  
  public NeighbourhoodGenerator(ArrayList<SensorNetworkQueryPlan> alternativePlans, 
                                TABUList tabuList)
  {
    this.alternativePlans = alternativePlans;
    this.tabuList = tabuList;
  }
  
  /**
   * generates the neighbourhood to be assessed. 
   * Uses aspiration plus to reduce size of candidates in the neighbourhood.
   * @param currentBestSuccessor 
   * @param position
   * @return
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws IOException 
   */
  public ArrayList<Successor> generateNeighbourHood(Successor currentBestSuccessor,
                                                     int position, int iteration) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException, IOException
  {
    ArrayList<Successor> neighbourHood = new ArrayList<Successor>();
    HashMap<SensorNetworkQueryPlan, Integer> alternativePlansUsed = new HashMap<SensorNetworkQueryPlan, Integer>();
    ArrayList<SensorNetworkQueryPlan> alternativePlansTemp = new ArrayList<SensorNetworkQueryPlan>();
    HashMap<SensorNetworkQueryPlan, Boolean> alternativePlansBestTimeUsed = new HashMap<SensorNetworkQueryPlan, Boolean>();
    
    alternativePlansTemp.addAll(this.alternativePlans);
    
    //remove all entire tabued plans
    Iterator<SensorNetworkQueryPlan> planIterator = alternativePlans.iterator();
    while(planIterator.hasNext())
    {
      SensorNetworkQueryPlan plan = planIterator.next();
      if(tabuList.isEntirelyTABU(plan, position))
      {
        alternativePlansTemp.remove(plan);
        int bestChance =  this.determineBestAgendaSwitch(plan, currentBestSuccessor, 
                                                         currentBestSuccessor.calculateLifetime());
        Successor successor = new Successor(plan, bestChance, currentBestSuccessor.getCopyOfRunTimeSites(), 
                                            currentBestSuccessor.getAgendaCount());
        if(tabuList.passesAspirationCriteria(successor, currentBestSuccessor))
          neighbourHood.add(successor);
      }
    }
    
    //search neighbourHood for candidates
    Random successorChooser = new Random();
    Random agendaSwitchChooser = new Random();
    while(neighbourHood.size() <= AspirationPlusBounds && alternativePlansTemp.size() > 0)
    {
      int overallPosition = successorChooser.nextInt(alternativePlansTemp.size());
      SensorNetworkQueryPlan plan = alternativePlansTemp.get(overallPosition);
      if(!tabuList.isTabu(plan, position))
      {
        Boolean usedBestTime = alternativePlansBestTimeUsed.get(plan);
        int agendaCount;
        if(usedBestTime == null || !usedBestTime)
        {
          agendaCount = this.determineBestAgendaSwitch(plan, currentBestSuccessor,
                                                       currentBestSuccessor.getLifetimeInAgendas());
          alternativePlansBestTimeUsed.put(plan, true);
        }
        else
          agendaCount = agendaSwitchChooser.nextInt(currentBestSuccessor.getLifetimeInAgendas());
        
        Successor successor = new Successor(plan, agendaCount, currentBestSuccessor.getCopyOfRunTimeSites(), 
                                            currentBestSuccessor.getAgendaCount());
        neighbourHood.add(successor);
        Integer currentTimeUsed = alternativePlansUsed.get(plan);
        if(currentTimeUsed == null)
          alternativePlansUsed.put(plan, 1);
        else
          alternativePlansUsed.put(plan, currentTimeUsed +1);
      }
      else
      {
        ArrayList<Integer> timesTabued = tabuList.getTABUTimes(plan, position);
        int newRandomTime ;
        do
        {
          Boolean usedBestTime = alternativePlansBestTimeUsed.get(plan);
          if(usedBestTime == null || !usedBestTime)
          {
            newRandomTime = this.determineBestAgendaSwitch(plan, currentBestSuccessor,
                                                         currentBestSuccessor.getLifetimeInAgendas());
            alternativePlansBestTimeUsed.put(plan, true);
          }
          else
            newRandomTime = agendaSwitchChooser.nextInt(currentBestSuccessor.getLifetimeInAgendas());
        }while(timesTabued.contains(newRandomTime));
          
        Successor successor = new Successor(plan, newRandomTime, currentBestSuccessor.getCopyOfRunTimeSites(),
                                            currentBestSuccessor.getAgendaCount());
        neighbourHood.add(successor);
        Integer currentTimeUsed = alternativePlansUsed.get(plan);
        if(currentTimeUsed == null)
          alternativePlansUsed.put(plan, 1);
        else
          alternativePlansUsed.put(plan, currentTimeUsed +1);
      }
      
      Iterator<SensorNetworkQueryPlan> plans = alternativePlansUsed.keySet().iterator();
      while(plans.hasNext())
      {
        SensorNetworkQueryPlan currentPlan = plans.next();
        int timesUsed = alternativePlansUsed.get(currentPlan);
        if(timesUsed > numberOfRandomTimes)
          alternativePlansTemp.remove(currentPlan);
      }
    }
    return neighbourHood;
  }

  private int determineBestAgendaSwitch(SensorNetworkQueryPlan altPlan,
                                        Successor currentBestSuccessor, int lifetimeInAgendas) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    
    int maxAgenda = currentBestSuccessor.getLifetimeInAgendas();
    int minAgenda = 1;
    
    do
    {
      int middle = Math.round((minAgenda + maxAgenda) /2);
      int left = middle - 1;
      
      HashMap<String, RunTimeSite> runTimeSites = currentBestSuccessor.getCopyOfRunTimeSites();
      Successor middleSuccessor = new Successor(altPlan, middle, runTimeSites, 
                                                currentBestSuccessor.getAgendaCount());
      runTimeSites = currentBestSuccessor.getCopyOfRunTimeSites();
      Successor leftSuccessor = new Successor(altPlan, left, runTimeSites,
                                              currentBestSuccessor.getAgendaCount());
      
      int middleLifetime = middleSuccessor.getLifetimeInAgendas() + middle;
      int leftLifetime = leftSuccessor.getLifetimeInAgendas() + left;
     
      if(middleLifetime >= leftLifetime)
      {
        minAgenda = left;
      }
      else
        maxAgenda = left;
    }while (minAgenda + 5 < maxAgenda);
    return minAgenda;
  }
  
}
