package uk.ac.manchester.cs.snee.manager.common;

import java.util.ArrayList;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class TemporalAdjustment
{

  private ArrayList<String> sitesAffected = new ArrayList<String>();
  private long adjustmentPosition = 0;
  private long adjustmentDuration = 0;
  
  public TemporalAdjustment(long adjustmentPosition, long adjustmentDuration, ArrayList<String> affectedSites)
  {
    this.adjustmentDuration = adjustmentDuration;
    this.adjustmentPosition = adjustmentPosition;
    this.sitesAffected = affectedSites;
  }
  
  public TemporalAdjustment()
  {
  }
  
  public void addAffectedSite(String affectedSite)
  {
    sitesAffected.add(affectedSite);
  }
  
  public ArrayList<String> getAffectedSites()
  {
    return sitesAffected;
  }
  
  public void setAffectedSites(ArrayList<String> affectedSites)
  {
    this.sitesAffected = affectedSites;
  }
  
  public long getAdjustmentPosition()
  {
    return adjustmentPosition;
  }

  public void removeSiteFromAffectedSites(String site)
  {
    sitesAffected.remove(site);
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
  
  public Iterator<String> affectedsitesIterator()
  {
    return sitesAffected.iterator();
  }
  
  public String toString()
  {
    String output = "";
    output = output.concat("S[");
    Iterator<String> siteIterator = affectedsitesIterator();
    while(siteIterator.hasNext())
    {
      String concat = siteIterator.next();
      if(siteIterator.hasNext())
        output = output.concat(concat + ", ");
      else
        output = output.concat(concat);
    }
    output = output.concat("] St[" + adjustmentPosition + "] By[" + adjustmentDuration + "]");
    return output;
  }
  
  
}
