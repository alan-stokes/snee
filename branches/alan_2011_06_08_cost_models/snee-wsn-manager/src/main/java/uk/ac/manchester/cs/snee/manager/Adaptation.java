package uk.ac.manchester.cs.snee.manager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class Adaptation
{
  private ArrayList<Site> reprogrammingSites;
  private ArrayList<Site> redirectedionSites;
  private ArrayList<TemporalAdjustment> temporalSites;
  private ArrayList<Site> deactivationSites;
  private ArrayList<Site> activateSites;
  private SensorNetworkQueryPlan newQep = null;
  private SensorNetworkQueryPlan oldQep = null;
  private Long timeCost = null;
  private HashMap<String, Long> siteEnergyCosts = new HashMap<String, Long>();
  private Long overallEnergyCost = null;
  private Long runtimeCost = null;
  private Double lifetimeEstimate = null; //done in agenda cycles
  private StrategyID id;
  private int numberID;
  
  public Adaptation(SensorNetworkQueryPlan oldQep, StrategyID id, int numberID)
  {
    reprogrammingSites = new ArrayList<Site>();
    redirectedionSites = new ArrayList<Site>();
    temporalSites = new ArrayList<TemporalAdjustment>();
    deactivationSites = new ArrayList<Site>();
    activateSites = new ArrayList<Site>();
    this.setOldQep(oldQep);
    this.setId(id);
    this.numberID = numberID;
  }
  
  /**
   * returns an arraylist of all sites which are affected by the temporal adjustments. 
   * does not reduce duplicates
   * @return
   */
  public ArrayList<Site> getSitesAffectedByAllTemporalChanges()
  {
	ArrayList<Site> affectedSites = new ArrayList<Site>();
	Iterator<TemporalAdjustment> adjustmentIterator = temporalSitesIterator();
	while(adjustmentIterator.hasNext())
	{
	  affectedSites.addAll(adjustmentIterator.next().getAffectedSites());
	}
	return affectedSites;
  }
  
  /**
   * gets the adjustment which contains site
   * @param site
   * @return
   */
  public TemporalAdjustment getAdjustmentContainingSite(Site site)
  {
	Iterator<TemporalAdjustment> adjustmentIterator = temporalSitesIterator();
	while(adjustmentIterator.hasNext())
	{
		TemporalAdjustment adjustment = adjustmentIterator.next();
	  if(adjustment.getAffectedSites().contains(site))
        return adjustment; 
	}
	return null;
  }
  
  public void addReprogrammedSite(Site site)
  {
    reprogrammingSites.add(site);
  }
  
  public void addRedirectedSite(Site site)
  {
    redirectedionSites.add(site);
  }
  
  public void addTemporalSite(TemporalAdjustment site)
  {
    temporalSites.add(site);
  }
  
  public void addDeactivatedSite(Site site)
  {
    deactivationSites.add(site);
  }
  
  public void addActivatedSite(Site site)
  {
    activateSites.add(site);
  }
  
  public Iterator<Site> reprogrammingSitesIterator()
  {
    return reprogrammingSites.iterator();
  }
  
  public Iterator<Site> redirectedionSitesIterator()
  {
    return redirectedionSites.iterator();
  }
  
  public Iterator<TemporalAdjustment> temporalSitesIterator()
  {
    return temporalSites.iterator();
  }
  
  public Iterator<Site> deactivationSitesIterator()
  {
    return deactivationSites.iterator();
  }
  
  public Iterator<Site> activateSitesIterator()
  {
    return activateSites.iterator();
  }

  public void setNewQep(SensorNetworkQueryPlan newQep)
  {
    this.newQep = newQep;
  }

  public SensorNetworkQueryPlan getNewQep()
  {
    return newQep;
  }
  
  public void setOldQep(SensorNetworkQueryPlan oldQep)
  {
    this.oldQep = oldQep;
  }

  public SensorNetworkQueryPlan getOldQep()
  {
    return oldQep;
  }
  
  public int getTemporalChangesSize()
  {
    return this.temporalSites.size();
  }
  
  public boolean reprogrammingContains(Site find)
  {
    return reprogrammingSites.contains(find);
  }
  
  public ArrayList<Site> getReprogrammingSites()
  {
    return reprogrammingSites;
  }

  public ArrayList<Site> getRedirectedionSites()
  {
    return redirectedionSites;
  }

  public ArrayList<TemporalAdjustment> getTemporalSites()
  {
    return temporalSites;
  }

  public ArrayList<Site> getDeactivationSites()
  {
    return deactivationSites;
  }

  public ArrayList<Site> getActivateSites()
  {
    return activateSites;
  }
  
  public void setTimeCost(Long timeCost)
  {
    this.timeCost = timeCost;
  }

  public Long getTimeCost()
  {
    return timeCost;
  }

  public void setEnergyCost(Long energyCost)
  {
    this.overallEnergyCost = energyCost;
  }

  public Long getEnergyCost()
  {
    return overallEnergyCost;
  }
  
  public void setId(StrategyID id)
  {
    this.id = id;
  }

  public StrategyID getStrategyId()
  {
    return id;
  }
  
  public String getOverallID()
  {
    return id.toString() + numberID;
  }
  
  public void setRuntimeCost(Long runtimeCost)
  {
    this.runtimeCost = runtimeCost;
  }

  public Long getRuntimeCost()
  {
    return runtimeCost;
  }

  public void setLifetimeEstimate(Double lifetimeEstimate)
  {
    this.lifetimeEstimate = lifetimeEstimate;
  }

  public Double getLifetimeEstimate()
  {
    return lifetimeEstimate;
  }
  
  public Long getSiteEnergyCost(Object key)
  {
    if(siteEnergyCosts.get(key) == null)
      return new Long(0);
    else
      return siteEnergyCosts.get(key);
  }

  public Long putSiteEnergyCost(String key, Long value)
  {
    return siteEnergyCosts.put(key, value);
  }
  
  
  /**
   * returns all offset start times of all temporal adjustments
   * @return
   */
  public ArrayList<Long> getTemporalDifferences()
  {
    ArrayList<Long> differences = new ArrayList<Long>();
    Iterator<TemporalAdjustment> temporalIterator = this.temporalSitesIterator();
    while(temporalIterator.hasNext())
    {
      differences.add(temporalIterator.next().getAdjustmentPosition());
    }
    return differences;
  }
  
  public String toString()
  {
    String output = "";
    Iterator<Site> reporgramIterator = reprogrammingSitesIterator();
    Iterator<Site> redirectedIterator = redirectedionSitesIterator();
    Iterator<TemporalAdjustment> temporalIterator = temporalSitesIterator();
    Iterator<Site> deactivatedIterator = deactivationSitesIterator();
    Iterator<Site> activateIterator = activateSites.iterator();
    output = output.concat("Reprogrammed[");
    while(reporgramIterator.hasNext())
    {
      String concat = reporgramIterator.next().getID();
      if(reporgramIterator.hasNext())
        output = output.concat(concat + ", ");
      else
        output = output.concat(concat);
    }
    output = output.concat("] Redirected[");
    while(redirectedIterator.hasNext())
    {
      String concat = redirectedIterator.next().getID();
      if(redirectedIterator.hasNext())
        output = output.concat(concat + ", ");
      else
        output = output.concat(concat);
    }
    output = output.concat("] Deactivated[");
    while(deactivatedIterator.hasNext())
    {
      String concat = deactivatedIterator.next().getID();
      if(deactivatedIterator.hasNext())
        output = output.concat(concat + ", ");
      else
        output = output.concat(concat);
    }
    output = output.concat("] activate[");
    while(activateIterator.hasNext())
    {
      String concat = activateIterator.next().getID();
      if(activateIterator.hasNext())
        output = output.concat(concat + ", ");
      else
        output = output.concat(concat);
    }
    output = output.concat("] Temporal[");
    int counter = 1;
    while(temporalIterator.hasNext())
    {
      output = output.concat("AD " + counter + " (");
      String concat = temporalIterator.next().toString();
      if(temporalIterator.hasNext())
        output = output.concat(concat + "), ");
      else
        output = output.concat(concat + ")");
      counter++;
    }
    output = output.concat("]");
    if(overallEnergyCost != null)
      output  = output.concat(" OverallEnergyCost [" + overallEnergyCost.toString() + "]");
    if(timeCost != null)
      output  = output.concat(" TimeCost [" + timeCost.toString() + "]");
    if(runtimeCost != null)
      output  = output.concat(" RunTimeCost [" + runtimeCost.toString() + "]");
    if(lifetimeEstimate != null)
      output  = output.concat(" lifetimeEstimate [" + lifetimeEstimate.toString() + " s]");
    output  = output.concat(" ID [" + id.toString() + ":" + numberID + "]");
    return output;
  }
}
