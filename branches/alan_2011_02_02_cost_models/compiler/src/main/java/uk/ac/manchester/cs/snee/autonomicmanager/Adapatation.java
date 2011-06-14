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
  
  
}
