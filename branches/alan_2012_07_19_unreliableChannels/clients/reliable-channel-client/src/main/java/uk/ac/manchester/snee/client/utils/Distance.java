package uk.ac.manchester.snee.client.utils;

import java.util.HashMap;

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
  
}
