package uk.ac.manchester.snee.client.collatetuples;

public class AverageAndVarianceKlevel
{
  private Double averageTuplePercentage = 0.0;
  
  private Double minTuplePercentage = Double.MAX_VALUE;
  
  private Double maxTuplePercentage = 0.0;
  
  public AverageAndVarianceKlevel()
  {
  }
  
  public String toString()
  {
    String output = averageTuplePercentage.toString() ;
                  
    return output;
  }
  
  public Double getMinTuplePercentage()
  {
    return minTuplePercentage;
  }
  
  public Double getMaTuplePercentage()
  {
    return maxTuplePercentage;
  }

 
  public Double getTuplePercentage()
  {
    return averageTuplePercentage;
  }

  public void addpercentage(Double tuplePercentage)
  {
    this.averageTuplePercentage += tuplePercentage;
    if(tuplePercentage < minTuplePercentage)
      minTuplePercentage=  tuplePercentage;
    if(tuplePercentage > maxTuplePercentage)
      maxTuplePercentage=  tuplePercentage;
    
  }

}
