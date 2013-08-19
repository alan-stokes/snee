package uk.ac.manchester.snee.client.autoPlotter;

public class Klevel
{
  private Double tuplePercentage = 0.0;
  private Double tuplePercentageMin =0.0;
  private Double tuplePercentageMax = 0.0;
  
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
  
  public static enum typeOfInput {TUPLES, LIFETIME};
  
  public Klevel(Double tuplePercentage)
  {
    this.tuplePercentage = tuplePercentage;
  }
  
  public Klevel(String line, typeOfInput typeOfInput)
  {
    if(typeOfInput.toString().equals("TUPLES"))
    {
      String [] bits = line.split(" ");
      tuplePercentage = Double.parseDouble(bits[1]);
      setTuplePercentageMin(Double.parseDouble(bits[2]));
      setTuplePercentageMax(Double.parseDouble(bits[3]));
    }
    else
    {
      String [] bits = line.split(" ");
      averageKe2Lifetime = Double.parseDouble(bits[1]);
      minKe2Lifetime = Double.parseDouble(bits[2]);
      maxKe2Lifetime = Double.parseDouble(bits[3]);
      averageKe3Lifetime = Double.parseDouble(bits[4]);
      minKe3Lifetime = Double.parseDouble(bits[5]);
      maxKe3Lifetime = Double.parseDouble(bits[6]);
      averageOptimsiticLifetime = Double.parseDouble(bits[7]);
      minOptimsiticLifetime = Double.parseDouble(bits[8]);
      maxOptimsiticLifetime = Double.parseDouble(bits[9]);
      averagePessimisticLifetime = Double.parseDouble(bits[10]);
      minPessimisticLifetime = Double.parseDouble(bits[11]);
      maxPessimisticLifetime = Double.parseDouble(bits[12]);
    }
  }
  
  public String toString()
  {
    String output = this.tuplePercentage.toString();
    return output;
  }
  
  public Double getTuplePercentage()
  {
    return this.tuplePercentage;
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

  public void addData(String line, typeOfInput typeofInput)
  {
    if(typeofInput.toString().equals("TUPLES"))
    {
      String [] bits = line.split(" ");
      tuplePercentage = Double.parseDouble(bits[1]);
      setTuplePercentageMin(Double.parseDouble(bits[2]));
      setTuplePercentageMax(Double.parseDouble(bits[3]));
    }
    else
    {
      String [] bits = line.split(" ");
      averageKe2Lifetime = Double.parseDouble(bits[1]);
      minKe2Lifetime = Double.parseDouble(bits[2]);
      maxKe2Lifetime = Double.parseDouble(bits[3]);
      averageKe3Lifetime = Double.parseDouble(bits[4]);
      minKe3Lifetime = Double.parseDouble(bits[5]);
      maxKe3Lifetime = Double.parseDouble(bits[6]);
      averageOptimsiticLifetime = Double.parseDouble(bits[7]);
      minOptimsiticLifetime = Double.parseDouble(bits[8]);
      maxOptimsiticLifetime = Double.parseDouble(bits[9]);
      averagePessimisticLifetime = Double.parseDouble(bits[10]);
      minPessimisticLifetime = Double.parseDouble(bits[11]);
      maxPessimisticLifetime = Double.parseDouble(bits[12]);
    }
  }
  
  public void addStaticData(String av, String min, String max)
  {
    averageStaticLifetime = Double.parseDouble(av);
    minStaticLifetime = Double.parseDouble(min);
    maxStaticLifetime = Double.parseDouble(max);
  }

  public void setTuplePercentageMin(Double tuplePercentageMin)
  {
    this.tuplePercentageMin = tuplePercentageMin;
  }

  public Double getTuplePercentageMin()
  {
    return tuplePercentageMin;
  }

  public void setTuplePercentageMax(Double tuplePercentageMax)
  {
    this.tuplePercentageMax = tuplePercentageMax;
  }

  public Double getTuplePercentageMax()
  {
    return tuplePercentageMax;
  }
  
}
