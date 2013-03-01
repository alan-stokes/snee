package uk.ac.manchester.snee.client.utils;

import java.util.HashMap;

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
  
  
  
}
