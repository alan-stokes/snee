package uk.ac.manchester.snee.client.collatelifetime;

public class AverageAndVarianceKlevel
{
  private Double averagePessimisticLifetime = 0.0;
  private Double averageOptimsiticLifetime = 0.0;
  private Double averageStaticLifetime = 0.0;
  private Double averageKe2Lifetime = 0.0;
  private Double averageKe3Lifetime = 0.0;
  
  private Double minPessimisticLifetime = Double.MAX_VALUE;
  private Double minOptimsiticLifetime = Double.MAX_VALUE;
  private Double minStaticLifetime = Double.MAX_VALUE;
  private Double minKe2Lifetime = Double.MAX_VALUE;
  private Double minKe3Lifetime = Double.MAX_VALUE;
  
  private Double maxPessimisticLifetime = 0.0;
  private Double maxOptimsiticLifetime = 0.0;
  private Double maxStaticLifetime = 0.0;
  private Double maxKe2Lifetime = 0.0;
  private Double maxKe3Lifetime = 0.0;
  
  public AverageAndVarianceKlevel()
  {
  }
  
  public String toString()
  {
    String output = averagePessimisticLifetime + "|" + averageOptimsiticLifetime + "|" + 
                    averageStaticLifetime + "|" + averageKe2Lifetime + "|" + averageKe3Lifetime + "|";
    return output;
  }
  
  public Double getPessimisticLifetime()
  {
    return averagePessimisticLifetime;
  }


  public Double getOptimsiticLifetime()
  {
    return averageOptimsiticLifetime;
  }


  public Double getStaticLifetime()
  {
    return averageStaticLifetime;
  }


  public Double getKe2Lifetime()
  {
    return averageKe2Lifetime;
  }


  public Double getKe3Lifetime()
  {
    return averageKe3Lifetime;
  }

  public void addPessimisticLifetime(Double pessimisticLifetime)
  {
    this.averagePessimisticLifetime += pessimisticLifetime;
    if(pessimisticLifetime < minPessimisticLifetime)
      minPessimisticLifetime=  pessimisticLifetime;
    if(pessimisticLifetime > maxPessimisticLifetime)
      maxPessimisticLifetime=  pessimisticLifetime;
  }

  public void addOptimsiticLifetime(Double optimsiticLifetime)
  {
    this.averageOptimsiticLifetime += optimsiticLifetime;
    if(optimsiticLifetime < minOptimsiticLifetime)
      minOptimsiticLifetime=  optimsiticLifetime;
    if(optimsiticLifetime > maxOptimsiticLifetime)
      maxOptimsiticLifetime=  optimsiticLifetime;
    
  }

  public void addKe2Lifetime(Double ke2Lifetime)
  {
    this.averageKe2Lifetime += ke2Lifetime;
    if(ke2Lifetime < minKe2Lifetime)
      minKe2Lifetime=  ke2Lifetime;
    if(ke2Lifetime > maxKe2Lifetime)
      maxKe2Lifetime=  ke2Lifetime;
  }
  
  public void addKe3Lifetime(Double ke3Lifetime)
  {
    this.averageKe3Lifetime += ke3Lifetime;
    if(ke3Lifetime < minKe3Lifetime)
      minKe3Lifetime=  ke3Lifetime;
    if(ke3Lifetime > maxKe3Lifetime)
      maxKe3Lifetime=  ke3Lifetime;
  }
  
  public void addStaticLifetime(Double staticLifetime)
  {
    this.averageStaticLifetime += staticLifetime;
    if(staticLifetime < minStaticLifetime)
      minStaticLifetime=  staticLifetime;
    if(staticLifetime > maxStaticLifetime)
      maxStaticLifetime=  staticLifetime;
  }

  public Double getMinPessimisticLifetime()
  {
    return minPessimisticLifetime;
  }

  public Double getMinOptimsiticLifetime()
  {
    return minOptimsiticLifetime;
  }

  public Double getMinStaticLifetime()
  {
    return minStaticLifetime;
  }

  public Double getMinKe2Lifetime()
  {
    return minKe2Lifetime;
  }

  public Double getMinKe3Lifetime()
  {
    return minKe3Lifetime;
  }

  public Double getMaxPessimisticLifetime()
  {
    return maxPessimisticLifetime;
  }

  public Double getMaxOptimsiticLifetime()
  {
    return maxOptimsiticLifetime;
  }

  public Double getMaxStaticLifetime()
  {
    return maxStaticLifetime;
  }

  public Double getMaxKe2Lifetime()
  {
    return maxKe2Lifetime;
  }

  public Double getMaxKe3Lifetime()
  {
    return maxKe3Lifetime;
  }
  
}
