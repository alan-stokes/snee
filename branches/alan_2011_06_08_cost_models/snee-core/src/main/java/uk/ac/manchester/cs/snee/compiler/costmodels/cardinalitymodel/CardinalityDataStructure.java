package uk.ac.manchester.cs.snee.compiler.costmodels.cardinalitymodel;

import uk.ac.manchester.cs.snee.compiler.costmodels.CostModelDataStructure;

public class CardinalityDataStructure extends CostModelDataStructure
{
  //streamCard
  private float cardOfStream = 0;
  private String extentName = "";
  private boolean stream = false;
  //stream of window
  private float windowStreamCard = 0;
  private float windowCard = 0;

  //stream/window constructor
  public CardinalityDataStructure(float card)
  {
    this.cardOfStream = card;
    stream = true;
  }
  
  //stream of windows constructor
  public CardinalityDataStructure(float windowStreamCard, float windowCard)
  {
    this.windowCard = windowCard;
    this.windowStreamCard = windowStreamCard;
    stream = false;
  }
  
  public float getWindowCard()
  {
    return windowCard;
  }

  public void setWindowCard(float windowCard)
  {
    this.windowCard = windowCard;
  }
  
  
  public String toString()
  {
    String output = "";
    if(stream)
      output = "Stream Cardinality is: " + cardOfStream;
    else
      output = windowStreamCard + " windows, each with " + windowCard + " tuples";

    return output;
  }
  
  public float getCardOfStream()
  {
    if(stream)
      return cardOfStream;
    else
      return windowStreamCard;
  }

  public void setCardOfStream(float cardOfStream)
  {
    if(stream)
	    this.cardOfStream = cardOfStream;
    else
      this.windowStreamCard = cardOfStream;
  }
  
  public boolean isStream()
  {
    return stream;
  }

  public boolean isStreamOfWindows()
  {
    return !stream;
  }
  
  public float getCard()
  {
    if(stream)
      return this.cardOfStream;
    else
      return this.windowStreamCard * this.windowCard;
  }

  public void setExtentName(String extentName) 
  {
	  this.extentName = extentName;
  }

  public String getExtentName() 
  {
	  return extentName;
  }

  public float getDirectCard() 
  {
    if(stream)
      return this.cardOfStream;
    else
      return this.windowStreamCard;
  }
  
  public void setStream(boolean streamValue)
  {
    stream = streamValue;
  }
  
}
