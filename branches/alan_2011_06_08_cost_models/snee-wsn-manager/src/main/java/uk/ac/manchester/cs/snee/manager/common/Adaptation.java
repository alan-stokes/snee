package uk.ac.manchester.cs.snee.manager.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class Adaptation implements Serializable
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 3094111092521934407L;
  
  private ArrayList<String> reprogrammingSites;
  private ArrayList<String> redirectedionSites;
  private ArrayList<TemporalAdjustment> temporalSites;
  private ArrayList<String> deactivationSites;
  private ArrayList<String> activateSites;
  private ArrayList<String> failedNodes;
  private SensorNetworkQueryPlan newQep = null;
  private SensorNetworkQueryPlan oldQep = null;
  private Long timeCost = null;
  private HashMap<String, Long> siteEnergyCosts = new HashMap<String, Long>();
  private Double overallEnergyCost = null;
  private Double runtimeCost = null;
  private Double lifetimeEstimate = null; //done in agenda cycles
  private StrategyIDEnum id;
  private int numberID;
  private String nodeIdWhichEndsQuery = null;
  
  public Adaptation(SensorNetworkQueryPlan oldQep, StrategyIDEnum id, int numberID)
  {
    reprogrammingSites = new ArrayList<String>();
    redirectedionSites = new ArrayList<String>();
    temporalSites = new ArrayList<TemporalAdjustment>();
    deactivationSites = new ArrayList<String>();
    activateSites = new ArrayList<String>();
    this.setOldQep(oldQep);
    this.setId(id);
    this.numberID = numberID;
  }
  
  /**
   * returns an arraylist of all sites which are affected by the temporal adjustments. 
   * does not reduce duplicates
   * @param affectedSites2 
   * @return
   */
  public ArrayList<String> getSitesAffectedByAllTemporalChanges()
  {
	ArrayList<String> affectedSites = new ArrayList<String>();
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
	  if(adjustment.getAffectedSites().contains(site.getID()))
        return adjustment; 
	}
	return null;
  }
  
  public void addReprogrammedSite(String site)
  {
    if(!this.deactivationSites.contains(site) && !this.activateSites.contains(site))
      reprogrammingSites.add(site);
  }
  
  public void addRedirectedSite(String site)
  {
    redirectedionSites.add(site);
  }
  
  public void addTemporalSite(TemporalAdjustment site)
  {
    temporalSites.add(site);
  }
  
  public void addDeactivatedSite(String site)
  {
    deactivationSites.add(site);
  }
  
  public void addActivatedSite(String site)
  {
    activateSites.add(site);
  }
  
  public Iterator<String> reprogrammingSitesIterator()
  {
    return reprogrammingSites.iterator();
  }
  
  public Iterator<String> redirectedionSitesIterator()
  {
    return redirectedionSites.iterator();
  }
  
  public Iterator<TemporalAdjustment> temporalSitesIterator()
  {
    return temporalSites.iterator();
  }
  
  public Iterator<String> deactivationSitesIterator()
  {
    return deactivationSites.iterator();
  }
  
  public Iterator<String> activateSitesIterator()
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
  
  public ArrayList<String> getReprogrammingSites()
  {
    return reprogrammingSites;
  }

  public ArrayList<String> getRedirectedionSites()
  {
    return redirectedionSites;
  }

  public ArrayList<TemporalAdjustment> getTemporalSites()
  {
    return temporalSites;
  }

  public ArrayList<String> getDeactivationSites()
  {
    return deactivationSites;
  }

  public ArrayList<String> getActivateSites()
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

  public void setEnergyCost(Double energyCost)
  {
    this.overallEnergyCost = energyCost;
  }

  public Double getEnergyCost()
  {
    return overallEnergyCost;
  }
  
  public void setId(StrategyIDEnum id)
  {
    this.id = id;
  }

  public StrategyIDEnum getStrategyId()
  {
    return id;
  }
  
  public String getOverallID()
  {
    return id.toString() + numberID;
  }
  
  public void setRuntimeCost(Double runtimeCost)
  {
    this.runtimeCost = runtimeCost;
  }

  public Double getRuntimeCost()
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
    Iterator<String> reporgramIterator = reprogrammingSitesIterator();
    Iterator<String> redirectedIterator = redirectedionSitesIterator();
    Iterator<TemporalAdjustment> temporalIterator = temporalSitesIterator();
    Iterator<String> deactivatedIterator = deactivationSitesIterator();
    Iterator<String> activateIterator = activateSites.iterator();
    output = output.concat("Reprogrammed[");
    while(reporgramIterator.hasNext())
    {
      String concat = reporgramIterator.next();
      if(reporgramIterator.hasNext())
        output = output.concat(concat + ", ");
      else
        output = output.concat(concat);
    }
    output = output.concat("] Redirected[");
    while(redirectedIterator.hasNext())
    {
      String concat = redirectedIterator.next();
      if(redirectedIterator.hasNext())
        output = output.concat(concat + ", ");
      else
        output = output.concat(concat);
    }
    output = output.concat("] Deactivated[");
    while(deactivatedIterator.hasNext())
    {
      String concat = deactivatedIterator.next();
      if(deactivatedIterator.hasNext())
        output = output.concat(concat + ", ");
      else
        output = output.concat(concat);
    }
    output = output.concat("] activate[");
    while(activateIterator.hasNext())
    {
      String concat = activateIterator.next();
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
      output  = output.concat(" OverallEnergyCost [" + overallEnergyCost.toString() + "j]");
    if(timeCost != null)
      output  = output.concat(" TimeCost [" + timeCost.toString() + "ms]");
    if(runtimeCost != null)
      output  = output.concat(" RunTimeCost [" + runtimeCost.toString() + "j]");
    if(lifetimeEstimate != null)
      output  = output.concat(" lifetimeEstimate [" + lifetimeEstimate.toString() + " ms]");
    output  = output.concat(" ID [" + id.toString() + ":" + numberID + "]");
    if(this.nodeIdWhichEndsQuery != null)
      output  = output.concat("LifetimeFailedNode[" + nodeIdWhichEndsQuery + "]");
    return output;
  }

  public void setFailedNodes(ArrayList<String> failedNodes)
  {
    this.failedNodes = failedNodes;
  }

  public ArrayList<String> getFailedNodes()
  {
    return failedNodes;
  }

  public void setNodeIdWhichEndsQuery(String nodeIdWhichEndsQuery)
  {
    this.nodeIdWhichEndsQuery = nodeIdWhichEndsQuery;
  }

  public String getNodeIdWhichEndsQuery()
  {
    return nodeIdWhichEndsQuery;
  }
}
