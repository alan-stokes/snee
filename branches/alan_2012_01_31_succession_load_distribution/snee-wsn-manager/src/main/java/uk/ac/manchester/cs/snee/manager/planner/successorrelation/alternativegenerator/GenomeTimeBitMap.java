package uk.ac.manchester.cs.snee.manager.planner.successorrelation.alternativegenerator;

import java.util.ArrayList;

public class GenomeTimeBitMap
{
  private int maxSegments = 32;
  private ArrayList<Integer> mapping = new ArrayList<Integer>(32);
  
  public GenomeTimeBitMap(int maxRange)
  {
    //if not enough slots, just make slots size 1
    if(maxRange < maxSegments)
      maxSegments = maxRange;
    double eachSegment = maxRange / maxSegments;
    double middle = eachSegment / 2;
    for(int index = 0; index < maxSegments; index++)
    {
      mapping.add((int) (middle + (eachSegment * index)));
    }
  }
  
  public Integer getTime(int mappingIndex)
  {
    return mapping.get(mappingIndex);
  }
  
  public Integer getMaxSegments()
  {
    return maxSegments;
  }
  
}
