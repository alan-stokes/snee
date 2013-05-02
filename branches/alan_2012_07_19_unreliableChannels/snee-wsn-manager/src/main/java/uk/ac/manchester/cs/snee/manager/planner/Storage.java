package uk.ac.manchester.cs.snee.manager.planner;



public class Storage
{
  private Double robustLifetime;
  private Double naiveLifetime;
  
  public Storage(Double rLife, Double nLife)
  {
    setRobustLifetime(rLife);
    setNaiveLifetime(nLife);
  }

  public void setRobustLifetime(Double robustLifetime)
  {
    this.robustLifetime = robustLifetime;
  }

  public Double getRobustLifetime()
  {
    return robustLifetime;
  }

  public void setNaiveLifetime(Double naiveLifetime)
  {
    this.naiveLifetime = naiveLifetime;
  }

  public Double getNaiveLifetime()
  {
    return naiveLifetime;
  }
  
  
}
