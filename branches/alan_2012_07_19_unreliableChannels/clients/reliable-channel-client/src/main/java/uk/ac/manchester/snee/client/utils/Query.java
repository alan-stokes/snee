package uk.ac.manchester.snee.client.utils;

import java.util.HashMap;
import java.util.Iterator;

public class Query
{
  private HashMap<String, Distance> data = new HashMap<String, Distance>();
  
  public Query(HashMap<String, Distance> data)
  {
    this.setData(data);
  }

  public void setData(HashMap<String, Distance> data)
  {
    this.data = data;
  }

  public HashMap<String, Distance> getData()
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
      output = output.concat(data.get(key).toString() + ":");
    }
    return output;
  }
  
}
