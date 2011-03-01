package uk.ac.manchester.cs.snee.compiler.costmodels;

public class CardinalityStruct
{
  private float noWindows = 0;
  private float cardPerWindow = 0;
  
  public CardinalityStruct(float noWindows, float cardPerWindow)
  {
    this.noWindows = noWindows;
    this.cardPerWindow = cardPerWindow;
  }

  public float getNoWindows()
  {
    return noWindows;
  }

  public void setNoWindows(float noWindows)
  {
    this.noWindows = noWindows;
  }

  public float getCardPerWindow()
  {
    return cardPerWindow;
  }

  public void setCardPerWindow(float cardPerWindow)
  {
    this.cardPerWindow = cardPerWindow;
  }
  
  public String toString()
  {
    String output =  "noX = " + noWindows + ": XCard = " + cardPerWindow;
    return output;
  }
}
