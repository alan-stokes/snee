package uk.ac.manchester.cs.snee.manager.planner.common;

import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;

import com.rits.cloning.Cloner;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.params.qos.QoSExpectations;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class Successor implements Comparable<Successor>
{
  protected SensorNetworkQueryPlan qep;
  protected Integer agendaCount;
  protected Integer previousAgendaCount = 0;
  private HashMap<String, RunTimeSite> newRunTimeSites = null;
  
  
  public Successor(SensorNetworkQueryPlan qep, Integer agendaCount, 
                   HashMap<String, RunTimeSite> RunTimeSites, Integer prevAgendaCount) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    this.setQep(qep);
    this.setAgendaCount(agendaCount); 
    this.setNewRunTimeSites(RunTimeSites);
    this.previousAgendaCount = prevAgendaCount;
    this.subtractWaitingSiteEnergyCosts();
    this.updateSitesRunningCosts();
  }
  
  private void updateSitesRunningCosts() 
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    Iterator<String> siteKeyIterator = this.getNewRunTimeSites().keySet().iterator();
    while(siteKeyIterator.hasNext())
    {
      String key = siteKeyIterator.next();
      RunTimeSite site = getNewRunTimeSites().get(key);
      Site rtSite = this.qep.getRT().getSite(site.toString());
      site.setQepExecutionCost(this.qep.getAgendaIOT().getSiteEnergyConsumption(rtSite));
    }   
  }

  public HashMap<String, RunTimeSite> getCopyOfRunTimeSites()
  {
    Cloner cloner = new Cloner();
    cloner.dontClone(Logger.class);
    return cloner.deepClone(getNewRunTimeSites());
  }
  
  public HashMap<String, RunTimeSite> getTheRunTimeSites()
  {
    return getNewRunTimeSites();
  }

  protected void setQep(SensorNetworkQueryPlan qep)
  {
    this.qep = qep;
  }

  public SensorNetworkQueryPlan getQep()
  {
    return qep;
  }

  protected void setAgendaCount(Integer agendaCount)
  {
    this.agendaCount = agendaCount;
  }

  public Integer getAgendaCount()
  {
    return agendaCount;
  }

  public Integer getLifetimeInAgendas()
  {
    return this.calculateLifetime() + agendaCount + this.previousAgendaCount;
  }
  
  public Integer getBasicLifetimeInAgendas()
  {
    return this.calculateLifetime();
  }
  
  public Integer getPreviousAgendaCount()
  {
    return previousAgendaCount;
  }

  /**
   * calculates how much energy will be lost per site whilst waiting for the successor.
   * @return
   */
  protected void subtractWaitingSiteEnergyCosts()
  {
    Iterator<Site> siteIterator = this.qep.getRT().siteIterator(TraversalOrder.POST_ORDER);
    while(siteIterator.hasNext())
    {
      Site site = siteIterator.next();
      Double siteEnergyCost = this.getNewRunTimeSites().get(site.getID()).getQepExecutionCost();
      siteEnergyCost = siteEnergyCost * this.agendaCount;
      getNewRunTimeSites().get(site.getID()).removeDefinedCost(siteEnergyCost);
    }
  }
  
  /**
   * removes the cost of adapting from one plan to another.
   * @param adapt
   */
  public void substractAdaptationCostOffRunTimeSites(Adaptation adapt)
  {
    Iterator<Site> siteIterator = this.qep.getRT().siteIterator(TraversalOrder.POST_ORDER);
    while(siteIterator.hasNext())
    {
      Site site = siteIterator.next();
      RunTimeSite runTimeSite = getNewRunTimeSites().get(site.getID());
      Double siteAdaptationCost = adapt.getSiteEnergyCost(site.getID());
      runTimeSite.removeDefinedCost(siteAdaptationCost);
    }
  }
  
  /**
   * Calculates the lifetime of the plan based off current energy model costs and conditions.
   */
  public int calculateLifetime()
  {
    int shortestLifetime = Integer.MAX_VALUE;
    Iterator<Site> siteIterator = this.qep.getRT().siteIterator(TraversalOrder.POST_ORDER);
    while(siteIterator.hasNext())
    {
      Site site = siteIterator.next();
      double siteEnergyCost = getNewRunTimeSites().get(site.getID()).getQepExecutionCost();
      double currentSiteEnergySupply = getNewRunTimeSites().get(site.getID()).getCurrentEnergy();
      Double siteLifetime = (currentSiteEnergySupply / siteEnergyCost);
      if(!site.getID().equals(this.qep.getRT().getRoot().getID()))
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

  @Override
  public int compareTo(Successor o)
  {
    return 0;
  }
  
  @Override
  public boolean equals(Object other)
  {
    if(other.toString().equals(this.toString()))
      return true;
    else
      return false;
  }
  
  @Override
  public String toString()
  {
    return this.qep.getID() + " AT " + this.getAgendaCount().toString();
  }

  public void setNewRunTimeSites(HashMap<String, RunTimeSite> newRunTimeSites)
  {
    this.newRunTimeSites = newRunTimeSites;
  }

  public HashMap<String, RunTimeSite> getNewRunTimeSites()
  {
    return newRunTimeSites;
  }
  
  public QoSExpectations getQoS()
  {
    return this.qep.getQos();
  }
}
