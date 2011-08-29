package uk.ac.manchester.cs.snee.manager.common;

import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class RunTimeSite
{
  private Double currentEnergy = new Double(0);
  private Double currentAdaptationEnergyCost = new Double (0);
  private Double qepExecutionCost = new Double(0);;
  private Site representativeSite;
  
  public RunTimeSite(Double currentEnergy, Site representativeSite, Double qepExecutionCost)
  {
    this.currentEnergy = currentEnergy;
    this.representativeSite = representativeSite;
    this.qepExecutionCost = qepExecutionCost;
  }
  
  public Double getQepExecutionCost()
  {
    return qepExecutionCost;
  }

  public void setQepExecutionCost(Double qepExecutionCost)
  {
    this.qepExecutionCost = qepExecutionCost;
  }
  
  public Double getCurrentEnergy()
  {
    return currentEnergy;
  }

  public void setCurrentEnergy(Double currentEnergy)
  {
    this.currentEnergy = currentEnergy;
  }

  public Double getCurrentAdaptationEnergyCost()
  {
    return currentAdaptationEnergyCost;
  }

  public void addToCurrentAdaptationEnergyCost(Double currentAdaptationEnergyCost)
  {
    this.currentAdaptationEnergyCost += currentAdaptationEnergyCost;
  }
  
  public void resetCurrentAdaptationEnergyCost()
  {
    this.currentAdaptationEnergyCost = new Double(0);
  }
  
  public void resetQEPExecutionEnergyCost()
  {
    this.qepExecutionCost = new Double(0);
  }
  
  public void resetEnergyCosts()
  {
    resetQEPExecutionEnergyCost();
    resetCurrentAdaptationEnergyCost();
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
