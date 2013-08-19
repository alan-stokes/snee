package uk.ac.manchester.snee.client.collatetuples;

public class Klevel
{
  private Double tuplePercentage = 0.0;

  
  public Klevel(Double tuplePercentage)
  {
    this.tuplePercentage = tuplePercentage;

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
  
}
