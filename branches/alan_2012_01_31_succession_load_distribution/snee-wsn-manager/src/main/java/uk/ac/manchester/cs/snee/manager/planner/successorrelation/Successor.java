package uk.ac.manchester.cs.snee.manager.planner.successorrelation;

import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;

import com.rits.cloning.Cloner;

import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class Successor
{
  private SensorNetworkQueryPlan qep;
  private Integer agendaCount;
  private HashMap<String, RunTimeSite> newRunTimeSites = null;
  private HashMap<String, RunTimeSite> previousRunTimeSites = null;
  
  
  public Successor(SensorNetworkQueryPlan qep, Integer agendaCount, 
                   HashMap<String, RunTimeSite> newRunTimeSites,
                   HashMap<String, RunTimeSite> previousRunTimeSites)
  {
    this.setQep(qep);
    this.setAgendaCount(agendaCount); 
    this.previousRunTimeSites = previousRunTimeSites;
    this.newRunTimeSites = newRunTimeSites;
    this.subtractWaitingSiteEnergyCosts();
  }
  
  public HashMap<String, RunTimeSite> getCopyOfRunTimeSites()
  {
    Cloner cloner = new Cloner();
    cloner.dontClone(Logger.class);
    return cloner.deepClone(newRunTimeSites);
  }

  private void setQep(SensorNetworkQueryPlan qep)
  {
    this.qep = qep;
  }

  public SensorNetworkQueryPlan getQep()
  {
    return qep;
  }

  private void setAgendaCount(Integer agendaCount)
  {
    this.agendaCount = agendaCount;
  }

  public Integer getAgendaCount()
  {
    return agendaCount;
  }

  public Integer getLifetimeInAgendas()
  {
    return this.calculateLifetime();
  }
  
  /**
   * calculates how much energy will be lost per site whilst waiting for the successor.
   * @param agendaCount2
   * @return
   */
  private void subtractWaitingSiteEnergyCosts()
  {
    Iterator<Site> siteIterator = this.qep.getRT().siteIterator(TraversalOrder.POST_ORDER);
    while(siteIterator.hasNext())
    {
      Site site = siteIterator.next();
      Double siteEnergyCost = previousRunTimeSites.get(site.getID()).getQepExecutionCost();
      siteEnergyCost = siteEnergyCost * this.agendaCount;
      newRunTimeSites.get(site.getID()).removeDefinedCost(siteEnergyCost);
    }
  }
  
  /**
   * removes the cost of adapting from one plan to another.
   * @param adapt
   */
  public void substractAdaptationCostOffRunTImeSites(Adaptation adapt)
  {
    Iterator<Site> siteIterator = this.qep.getRT().siteIterator(TraversalOrder.POST_ORDER);
    while(siteIterator.hasNext())
    {
      Site site = siteIterator.next();
      RunTimeSite runTimeSite = newRunTimeSites.get(new Integer(site.getID()));
      Double siteAdaptationCost = adapt.getSiteEnergyCost(site.getID());
      runTimeSite.removeDefinedCost(siteAdaptationCost);
    }
  }
  
  /**
   * Calculates the lifetime of the plan based off current energy model costs and conditions.
   */
  private int calculateLifetime()
  {
    int shortestLifetime = Integer.MAX_VALUE;
    Iterator<Site> siteIterator = this.qep.getRT().siteIterator(TraversalOrder.POST_ORDER);
    while(siteIterator.hasNext())
    {
      Site site = siteIterator.next();
      double siteEnergyCost = newRunTimeSites.get(new Integer(site.getID())).getQepExecutionCost();
      double currentSiteEnergySupply = newRunTimeSites.get(new Integer(site.getID())).getCurrentEnergy();
      Double siteLifetime = (currentSiteEnergySupply / siteEnergyCost);
      //uncomment out sections to not take the root site into account
      if (site!=this.qep.getIOT().getRT().getRoot()) 
      { 
        if(shortestLifetime > siteLifetime)
        {
          if(!site.isDeadInSimulation())
          {
            shortestLifetime = Math.round(siteLifetime.floatValue());
          }
        }
      }
    }
    return shortestLifetime;
  }
}
