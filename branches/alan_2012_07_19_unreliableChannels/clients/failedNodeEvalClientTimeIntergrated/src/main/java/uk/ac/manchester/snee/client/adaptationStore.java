package uk.ac.manchester.snee.client;

import java.util.ArrayList;

public class adaptationStore
{
  private ArrayList<Double> energyCost = new ArrayList<Double>();
  private ArrayList<Double> timeCost = new ArrayList<Double>();
  private ArrayList<Double> lifetime = new ArrayList<Double>();
  
  public adaptationStore(ArrayList<Double> energy, ArrayList<Double> time, ArrayList<Double> life)
  {
    this.setEnergyCost(energy);
    this.setTimeCost(time);
    this.setLifetime(life);
  }
  
  public adaptationStore()
  {
    for(int index = 0; index<8; index++)
    {
      energyCost.add(null);
      timeCost.add(null);
      lifetime.add(null);
    }
  }

  public Double setEnergy(int index, Double element)
  {
    return energyCost.set(index, element);
  }
  
  public Double setTime(int index, Double element)
  {
    return this.timeCost.set(index, element);
  }
  
  public Double setLife(int index, Double element)
  {
    return this.lifetime.set(index, element);
  }

  public void setEnergyCost(ArrayList<Double> energyCost)
  {
    this.energyCost = energyCost;
  }

  public ArrayList<Double> getEnergyCost()
  {
    return energyCost;
  }

  public void setTimeCost(ArrayList<Double> timeCost)
  {
    this.timeCost = timeCost;
  }

  public ArrayList<Double> getTimeCost()
  {
    return timeCost;
  }

  public void setLifetime(ArrayList<Double> lifetime)
  {
    this.lifetime = lifetime;
  }

  public ArrayList<Double> getLifetime()
  {
    return lifetime;
  }

}
