package uk.ac.manchester.cs.snee.autonomicmanager;

import java.util.ArrayList;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class Adapatation
{
  private ArrayList<Site> reprogrammingSites;
  private ArrayList<Site> redirectedionSites;
  private ArrayList<Site> temporalSites;
  private ArrayList<Site> deactivationSites;
  private long adjustmentPosition = 0;
  private long adjustmentDuration = 0;
  private SensorNetworkQueryPlan newQep = null;
  private SensorNetworkQueryPlan oldQep = null;
  
  public Adapatation(SensorNetworkQueryPlan oldQep)
  {
    reprogrammingSites = new ArrayList<Site>();
    redirectedionSites = new ArrayList<Site>();
    temporalSites = new ArrayList<Site>();
    deactivationSites = new ArrayList<Site>();
    this.oldQep = oldQep;
  }
  
  public void addReprogrammedSite(Site site)
  {
    reprogrammingSites.add(site);
  }
  
  public void addRedirectedSite(Site site)
  {
    redirectedionSites.add(site);
  }
  
  public void addTemporalSite(Site site)
  {
    temporalSites.add(site);
  }
  
  public void addDeactivatedSite(Site site)
  {
    deactivationSites.add(site);
  }
  
  public Iterator<Site> reprogrammingSitesIterator()
  {
    return reprogrammingSites.iterator();
  }
  
  public Iterator<Site> redirectedionSitesIterator()
  {
    return redirectedionSites.iterator();
  }
  
  public Iterator<Site> temporalSitesIterator()
  {
    return temporalSites.iterator();
  }
  
  public Iterator<Site> deactivationSitesIterator()
  {
    return deactivationSites.iterator();
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
  
  public long getAdjustmentPosition()
  {
    return adjustmentPosition;
  }

  public void setAdjustmentPosition(long adjustmentPosition)
  {
    this.adjustmentPosition = adjustmentPosition;
  }

  public long getAdjustmentDuration()
  {
    return adjustmentDuration;
  }

  public void setAdjustmentDuration(long adjustmentDuration)
  {
    this.adjustmentDuration = adjustmentDuration;
  }
  
  public String toString()
  {
    String output = "";
    Iterator<Site> reporgramIterator = reprogrammingSitesIterator();
    Iterator<Site> redirectedIterator = redirectedionSitesIterator();
    Iterator<Site> temporalIterator = temporalSitesIterator();
    Iterator<Site> deactivatedIterator = deactivationSitesIterator();
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
    output = output.concat("] Temporal[");
    while(temporalIterator.hasNext())
    {
      String concat = temporalIterator.next().getID();
      if(temporalIterator.hasNext())
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
    output = output.concat("]");
    return output;
  }
  
  
}
