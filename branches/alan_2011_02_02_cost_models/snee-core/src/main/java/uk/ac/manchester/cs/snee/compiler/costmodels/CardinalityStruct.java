package uk.ac.manchester.cs.snee.compiler.costmodels;

import java.util.ArrayList;
import java.util.Iterator;

public class CardinalityStruct
{
  //stream 
  private float cardOfStream = 0;
  boolean stream = false;
  //window
  private float windowCard = 0;
  private float windowID = 0;

  boolean window = false;
  //stream of window
  private ArrayList<CardinalityStruct> windows = new ArrayList<CardinalityStruct>();

  boolean streamOfWindows = false;

  //stream/window constructor
  public CardinalityStruct(float card)
  {
    this.cardOfStream = card;
    stream = true;
  }
  
  public CardinalityStruct(float card, float windowID)
  {
    this.windowCard = card;
    this.windowID = windowID;
  }
  
  //stream of windows constructor
  public CardinalityStruct(ArrayList<CardinalityStruct> windows)
  {
    this.windows = windows;
    streamOfWindows = true;
  }
  //stream of windows constructor
  public CardinalityStruct()
  {
    streamOfWindows = true;
  }

  
  public float getNoWindows()
  {
    return windows.size();
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
    else if(window)
      output = "window Cardinality is: " + windowCard;
    else
      output = windows.size() + " windows, each with " + windows.get(0).windowCard + " tuples";

    return output;
  }
  
  public float getCardOfStream()
  {
    return cardOfStream;
  }
  
  public CardinalityStruct get(int index)
  {
    return windows.get(index);
  }

  public void addWindow(CardinalityStruct e)
  {
     windows.add(e);
  }

  public void setCardOfStream(float cardOfStream)
  {
    this.cardOfStream = cardOfStream;
  }
  
  public boolean isStream()
  {
    return stream;
  }
  public boolean isWindow()
  {
    return window;
  }
  public boolean isStreamOfWindows()
  {
    return streamOfWindows;
  }
  
  public Iterator<CardinalityStruct> windowIterator()
  {
    return windows.iterator();
  }
  
  public float getWindowID()
  {
    return windowID;
  }
  
  public float getCard()
  {
    if(stream)
      return this.cardOfStream;
    else if(window)
      return this.windowCard;
    else
      return this.windows.size() * this.windows.get(0).windowCard;
  }
}
