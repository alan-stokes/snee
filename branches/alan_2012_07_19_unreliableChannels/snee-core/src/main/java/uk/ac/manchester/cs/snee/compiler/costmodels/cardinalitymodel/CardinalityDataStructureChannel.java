package uk.ac.manchester.cs.snee.compiler.costmodels.cardinalitymodel;

import java.util.ArrayList;

import uk.ac.manchester.cs.snee.compiler.costmodels.CostModelDataStructure;

public class CardinalityDataStructureChannel extends CostModelDataStructure
{
  //streamCard
  //stream of window
  private ArrayList<Window> windowStreamCard = new ArrayList<Window>();

  //stream of windows constructor
  public CardinalityDataStructureChannel(ArrayList<Window> windows)
  {
    windowStreamCard = windows;
  }
    
  public String toString()
  {
    String output = "";
    for(int index =0; index < windowStreamCard.size(); index++)
    {
      output = output.concat(windowStreamCard.get(index).getWindowID() + "-" + 
                             windowStreamCard.get(index).getTuples() + " : ");
    }
    return output;
  } 
  
  public ArrayList<Window> getWindows()
  {
    return this.windowStreamCard;
  }
}
