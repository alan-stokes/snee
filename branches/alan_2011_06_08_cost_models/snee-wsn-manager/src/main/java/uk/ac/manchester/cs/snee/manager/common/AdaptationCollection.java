package uk.ac.manchester.cs.snee.manager.common;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AdaptationCollection
{
  private ArrayList<Adaptation> PartialAdaptations;
  private Adaptation GlobalAdaptation = null;
  private ArrayList<Adaptation> LocalAdaptations;
  
  public AdaptationCollection()
  {
    PartialAdaptations = new ArrayList<Adaptation>();
    LocalAdaptations = new ArrayList<Adaptation>();
  }
  
  public void add(Adaptation ad)
  {
    String id = ad.getOverallID();
    String [] splitGlobal = id.split(StrategyID.FailedNodeGlobal.toString());
    String [] splitPartial = id.split(StrategyID.FailedNodePartial.toString());
    String [] splitLocal = id.split(StrategyID.FailedNodeLocal.toString());
    
    if(splitGlobal.length > 1)
      setGlobalAdaptation(ad);
    else if(splitPartial.length > 1)
    {
      PartialAdaptations.add(ad);
    }
    else if(splitLocal.length > 1)
    {
      LocalAdaptations.add(ad);
    }
  }
  
  public void addAll(List<Adaptation> ads)
  {
    Iterator<Adaptation> adsIterator = ads.iterator();
    while(adsIterator.hasNext())
    {
      Adaptation ad = adsIterator.next();
      this.add(ad);
    }
  }

  public ArrayList<Adaptation> getPartialAdaptations()
  {
    return PartialAdaptations;
  }

  public void setPartialAdaptations(ArrayList<Adaptation> partialAdaptations)
  {
    PartialAdaptations = partialAdaptations;
  }

  public ArrayList<Adaptation> getLocalAdaptations()
  {
    return LocalAdaptations;
  }

  public void setLocalAdaptations(ArrayList<Adaptation> localAdaptations)
  {
    LocalAdaptations = localAdaptations;
  }

  public void setGlobalAdaptation(Adaptation globalAdaptation)
  {
    GlobalAdaptation = globalAdaptation;
  }

  public Adaptation getGlobalAdaptation()
  {
    return GlobalAdaptation;
  }
  
  public List<Adaptation> getAll()
  {
    ArrayList<Adaptation> ads = new ArrayList<Adaptation>();
    ads.add(GlobalAdaptation);
    ads.addAll(LocalAdaptations);
    ads.addAll(PartialAdaptations);
    return ads;
  }
  
  public int getSize()
  {
    int numberOfAdaptations = 0;
    if(GlobalAdaptation != null)
      numberOfAdaptations++;
    numberOfAdaptations = numberOfAdaptations + PartialAdaptations.size() + 
                          LocalAdaptations.size();
    return numberOfAdaptations;
      
  }
  
}
