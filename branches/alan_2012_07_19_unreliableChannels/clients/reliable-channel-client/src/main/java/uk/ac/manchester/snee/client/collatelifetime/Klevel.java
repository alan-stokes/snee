package uk.ac.manchester.snee.client.collatelifetime;

public class Klevel
{
  private Double pessimisticLifetime = 0.0;
  private Double optimsiticLifetime = 0.0;
  private Double staticLifetime = 0.0;
  private Double ke2Lifetime = 0.0;
  private Double ke3Lifetime = 0.0;
  
  public Klevel(String dataLine)
  {
    String [] bits = dataLine.split("\t");
    pessimisticLifetime = Double.parseDouble(bits[4]);
    optimsiticLifetime = Double.parseDouble(bits[3]);
    staticLifetime = Double.parseDouble(bits[5]);
    ke2Lifetime = Double.parseDouble(bits[7]);
    ke3Lifetime = Double.parseDouble(bits[6]);
  }
  
  public String toString()
  {
    String output = pessimisticLifetime + "|" + optimsiticLifetime + "|" + 
                    staticLifetime + "|" + ke2Lifetime + "|" + ke3Lifetime + "|";
    return output;
  }
  
  public Double getPessimisticLifetime()
  {
    return pessimisticLifetime;
  }


  public Double getOptimsiticLifetime()
  {
    return optimsiticLifetime;
  }


  public Double getStaticLifetime()
  {
    return staticLifetime;
  }


  public Double getKe2Lifetime()
  {
    return ke2Lifetime;
  }


  public Double getKe3Lifetime()
  {
    return ke3Lifetime;
  }
  
}
