package uk.ac.manchester.cs.snee.manager.planner.successorrelation.alternativegenerator;

import java.util.ArrayList;

public class GenomeTimeBitMap
{
  private int maxSegments = 32;
  private ArrayList<Integer> mapping = new ArrayList<Integer>(32);
  
  public GenomeTimeBitMap(int maxRange)
  {
    System.out.println(" segments with maxRange = " + maxRange );
    //if not enough slots, just make slots size 1
    if(maxRange < maxSegments)
      maxSegments = maxRange;
    double eachSegment = maxRange / maxSegments;
    for(int index = 0; index <= maxSegments; index++)
    {
      mapping.add((int) (eachSegment * index));
      System.out.println("segment " + index + " is = " +  eachSegment * index);
    }
    //sort out loss of precision
    mapping.set(maxSegments -1, maxRange);
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
