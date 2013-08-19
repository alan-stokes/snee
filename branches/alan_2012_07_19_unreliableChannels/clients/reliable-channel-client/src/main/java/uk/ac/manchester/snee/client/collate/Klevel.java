package uk.ac.manchester.snee.client.collate;

import java.util.HashMap;
import java.util.Iterator;

public class Klevel
{
  private HashMap<String, Values> data = new HashMap<String, Values>();
 
  public Klevel(HashMap<String, Values> data)
  {
    this.setData(data);
  }

  public void setData(HashMap<String, Values> data)
  {
    this.data = data;
  }

  public HashMap<String, Values> getData()
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
