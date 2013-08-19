package uk.ac.manchester.snee.client.collate;
import java.util.HashMap;
import java.util.Iterator;

public class Distance
{
  private HashMap<String, Klevel> data = new HashMap<String, Klevel>();
 
  public Distance(HashMap<String, Klevel> data)
  {
    this.setData(data);
  }

  public void setData(HashMap<String, Klevel> data)
  {
    this.data = data;
  }

  public HashMap<String, Klevel> getData()
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

