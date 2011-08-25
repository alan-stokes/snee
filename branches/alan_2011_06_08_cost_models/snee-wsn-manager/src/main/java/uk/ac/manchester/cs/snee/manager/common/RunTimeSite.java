package uk.ac.manchester.cs.snee.manager.common;

import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class RunTimeSite
{
  private Long currentEnergy;
  private Long currentAdaptationEnergyCost;
  private Long qepExecutionCost;
  private Site representativeSite;
  
  public RunTimeSite(Long currentEnergy, Site representativeSite, Long qepExecutionCost)
  {
    this.currentEnergy = currentEnergy;
    this.representativeSite = representativeSite;
    this.qepExecutionCost = qepExecutionCost;
  }
  
  public Long getQepExecutionCost()
  {
    return qepExecutionCost;
  }

  public void setQepExecutionCost(Long qepExecutionCost)
  {
    this.qepExecutionCost = qepExecutionCost;
  }
  
  public Long getCurrentEnergy()
  {
    return currentEnergy;
  }

  public void setCurrentEnergy(Long currentEnergy)
  {
    this.currentEnergy = currentEnergy;
  }

  public Long getCurrentAdaptationEnergyCost()
  {
    return currentAdaptationEnergyCost;
  }

  public void addToCurrentAdaptationEnergyCost(Long currentAdaptationEnergyCost)
  {
    this.currentAdaptationEnergyCost += currentAdaptationEnergyCost;
  }
  
  public void resetCurrentAdaptationEnergyCost()
  {
    this.currentAdaptationEnergyCost = new Long(0);
  }

  public Site getRepresentativeSite()
  {
    return representativeSite;
  }

  public void setRepresentativeSite(Site representativeSite)
  {
    this.representativeSite = representativeSite;
  }

  public void removeQEPExecutionCost()
  {
    currentEnergy = currentEnergy - qepExecutionCost;
  }
  
}
