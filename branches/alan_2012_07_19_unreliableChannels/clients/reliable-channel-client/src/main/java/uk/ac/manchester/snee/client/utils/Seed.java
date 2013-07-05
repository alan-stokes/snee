package uk.ac.manchester.snee.client.utils;

import java.util.ArrayList;
import java.util.Iterator;

public class Seed
{
  private ArrayList<String> tuples = new ArrayList<String>();
  private ArrayList<Double> oAverage = new ArrayList<Double>();
  private ArrayList<Double> aAverage = new ArrayList<Double>();
  private ArrayList<Double> oAAverage = new ArrayList<Double>();
  private ArrayList<Double> aAAverage = new ArrayList<Double>();
  private ArrayList<Double> sAverage = new ArrayList<Double>();
  private Integer max = null;
  
  public Seed()
  {
    
  }

  public void setTuples(ArrayList<String> tuples)
  {
    this.tuples = tuples;
  }

  public ArrayList<String> getTuples()
  {
    return tuples;
  }

  public void setoAverage(ArrayList<Double> oAverage)
  {
    this.oAverage = oAverage;
  }

  public ArrayList<Double> getoAverage()
  {
    return oAverage;
  }

  public void setaAverage(ArrayList<Double> aAverage)
  {
    this.aAverage = aAverage;
  }

  public ArrayList<Double> getaAverage()
  {
    return aAverage;
  }

  public void setMax(Integer max)
  {
    this.max = max;
  }

  public Integer getMax()
  {
    return max;
  }

  public void setoAAverage(ArrayList<Double> oAAverage)
  {
    this.oAAverage = oAAverage;
  }

  public ArrayList<Double> getoAAverage()
  {
    return oAAverage;
  }

  public void setaAAverage(ArrayList<Double> aAAverage)
  {
    this.aAAverage = aAAverage;
  }

  public ArrayList<Double> getaAAverage()
  {
    return aAAverage;
  }
  
  public String toString()
  {
    String output = tuples.toString();
    output = output.concat("|"+this.getaAverage()+"|"+this.getoAverage()+"|"+this.aAAverage+"|"+this.oAAverage);
    return output;
  }

  public void setsAverage(ArrayList<Double> sAverage)
  {
    this.sAverage = sAverage;
  }

  public ArrayList<Double> getsAverage()
  {
    return sAverage;
  }
  
  
}
