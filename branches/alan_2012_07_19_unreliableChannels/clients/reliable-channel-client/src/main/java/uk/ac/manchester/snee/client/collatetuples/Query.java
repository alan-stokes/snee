package uk.ac.manchester.snee.client.collatetuples;

import java.util.HashMap;
import java.util.Iterator;

public class Query
{
  private HashMap<Integer, Klevel> data = new HashMap<Integer, Klevel>();
  
  public Query(HashMap<Integer, Klevel> data)
  {
    this.setData(data);
  }

  public void setData(HashMap<Integer, Klevel> data)
  {
    this.data = data;
  }

  public HashMap<Integer, Klevel> getData()
  {
    return data;
  }
  
  public String toString()
  {
    String output = "";
    Iterator<Integer> keys = data.keySet().iterator();
    while(keys.hasNext())
    {
      Integer key = keys.next();
      output = output.concat(data.get(key).toString() + ":");
    }
    return output;
  }
  
}
