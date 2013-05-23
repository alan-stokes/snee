package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.channel;

//class ported from Hyungjune lee from stanford. 

import java.util.ArrayList;

public class NoiseHash
{
  private String key = "";
  private int numElements;
  private int size;
  private ArrayList<Integer> elements = new ArrayList<Integer>();
  private boolean flag = false;
  private ArrayList<Double> dist = new ArrayList<Double>(NoiseModelConstants.NOISE_NUM_VALUES);
  private boolean combined = false;
  /**
   * constructor
   */
  public NoiseHash()
  {
    this.size = NoiseModelConstants.NOISE_DEFAULT_ELEMENT_SIZE;
    for(int index = 0; index < size; index++)
    {
      elements.add(index, 0);
    }
    for(int index = 0; index < NoiseModelConstants.NOISE_BIN_SIZE; index++)
    {
      dist.add(0.0);
    } 
  }
  
  public NoiseHash(String key, int numElements)
  {
    this.key = key;
    this.numElements = numElements;
    this.size = NoiseModelConstants.NOISE_DEFAULT_ELEMENT_SIZE;
    for(int index = 0; index < size; index++)
    {
      elements.add(index, 0);
    }
    for(int index = 0; index < NoiseModelConstants.NOISE_NUM_VALUES; index++)
    {
      dist.add(0.0);
    } 
  }

  /**
   * 
   *getter and setters 
   */
  
  public String getKey()
  {
    return key;
  }

  public void setKey(String key)
  {
    this.key = key;
  }

  public int getNumElements()
  {
    return numElements;
  }

  public void setNumElements(int numElements)
  {
    this.numElements = numElements;
  }

  public int getSize()
  {
    return size;
  }

  public void setSize(int size)
  {
    this.size = size;
  }

  public ArrayList<Integer> getElements()
  {
    return elements;
  }

  public void setElements(ArrayList<Integer> elements)
  {
    this.elements = elements;
  }

  public Boolean getFlag()
  {
    return flag;
  }

  public void setFlag(Boolean flag)
  {
    this.flag = flag;
  }

  public ArrayList<Double> getDist()
  {
    return dist;
  }

  public void addElement(Integer noiseValue)
  {
    try{
    if(this.numElements == this.size)
    {
      for(int index = 0; index < 10; index++)
      {
        elements.add(0);
      }
      size = size + 10;
    }
    elements.set(numElements, noiseValue);
    numElements++;
    }
    catch(Exception e)
    {
      System.out.println(numElements);
    }
  }

  public void setCombined(boolean combined)
  {
    this.combined = combined;
  }

  public boolean isCombined()
  {
    return combined;
  }
}
