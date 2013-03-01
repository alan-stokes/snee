package uk.ac.manchester.snee.client.utils;

import java.util.HashMap;

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
}
