package uk.ac.manchester.cs.snee.manager;

import java.util.ArrayList;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class Adapatation
{
  private ArrayList<Site> reprogrammingSites;
  private ArrayList<Site> redirectedionSites;
  private ArrayList<TemporalAdjustment> temporalSites;
  private ArrayList<Site> deactivationSites;
  private ArrayList<Site> activateSites;
  private SensorNetworkQueryPlan newQep = null;
  private SensorNetworkQueryPlan oldQep = null;
  
  public Adapatation(SensorNetworkQueryPlan oldQep)
  {
    reprogrammingSites = new ArrayList<Site>();
    redirectedionSites = new ArrayList<Site>();
    temporalSites = new ArrayList<TemporalAdjustment>();
    deactivationSites = new ArrayList<Site>();
    activateSites = new ArrayList<Site>();
    this.oldQep = oldQep;
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
    return output;
  }
  
  
}
