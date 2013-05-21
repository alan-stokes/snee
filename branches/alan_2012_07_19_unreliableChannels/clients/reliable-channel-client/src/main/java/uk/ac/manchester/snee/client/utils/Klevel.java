package uk.ac.manchester.snee.client.utils;

import java.util.HashMap;
import java.util.Iterator;

public class Klevel
{
  private HashMap<String, Seed> data = new HashMap<String, Seed>();
  
  public Klevel(HashMap<String, Seed> data)
  {
    this.setData(data);
  }

  public void setData(HashMap<String, Seed> data)
  {
    this.data = data;
  }

  public HashMap<String, Seed> getData()
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
