package uk.ac.manchester.snee.client.collateUnreliableLifetime;

import java.util.HashMap;
import java.util.Iterator;

public class Klevel
{
  private HashMap<String, Double> data = new HashMap<String, Double>();
 
  public Klevel(HashMap<String, Double> data)
  {
    this.setData(data);
  }

  public void setData(HashMap<String, Double> data)
  {
    this.data = data;
  }

  public HashMap<String, Double> getData()
  {
    return data;
  }
 
  public String toString()
  {
    String output = "";
    Iterator<String> keys = data.keySet().iterator();
    while(keys.hasNext())
    {
      String key = keys.next();
      output = output.concat(key + "-" + data.get(key).toString() + ":");
    }
    return output;
  }
}
