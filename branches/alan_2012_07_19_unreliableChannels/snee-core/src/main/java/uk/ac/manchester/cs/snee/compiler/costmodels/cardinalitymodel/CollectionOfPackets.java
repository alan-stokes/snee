package uk.ac.manchester.cs.snee.compiler.costmodels.cardinalitymodel;

import java.util.ArrayList;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.costmodels.HashMapList;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceExchangePart;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceOperator;


public class CollectionOfPackets
{
  private HashMapList<InstanceOperator, Window> windows = new HashMapList<InstanceOperator, Window>();
  
  
  public CollectionOfPackets(HashMapList<InstanceOperator, Window> packets)
  {
    this.windows = packets;
  }
  
  public CollectionOfPackets()
  {
  }
  
  public void updateCollection(InstanceOperator operator, final ArrayList<Window> cWindows)
  {
    ArrayList<Window> extentWindows = this.windows.get(operator);
    Iterator<Window> newWindows = cWindows.iterator();
    while(newWindows.hasNext())
    {
      Window newWindow = newWindows.next();
      Window oldWindow = getWindow(newWindow.getWindowID(), extentWindows);
      Window addedWindow = new Window(oldWindow.getTuples() + newWindow.getTuples(), newWindow.getWindowID());
      if(this.windows.keySet().contains(operator))
        this.windows.remove(operator, oldWindow);
      this.windows.add(operator, addedWindow);
    }
  }
  
  public void updateCollection(InstanceOperator extent, final ArrayList<Window> cWindows,
                               HashMapList<InstanceOperator, Window> windows)
  {
    ArrayList<Window> extentWindows = windows.get(extent);
    Iterator<Window> newWindows = cWindows.iterator();
    while(newWindows.hasNext())
    {
      Window newWindow = newWindows.next();
      Window oldWindow = getWindow(newWindow.getWindowID(), extentWindows);
      Window addedWindow = new Window(oldWindow.getTuples() + newWindow.getTuples(), newWindow.getWindowID());
      if(windows.keySet().contains(extent))
        windows.remove(extent, oldWindow);
      windows.add(extent, addedWindow);
    }
  }
  
  public ArrayList<Window> getWindowsOfExtent(InstanceOperator operator, String extent)
  {
    HashMapList<InstanceOperator, Window> allInputWindows = new HashMapList<InstanceOperator, Window>();
    Iterator<Node> inputIterator = operator.getInputsList().iterator();
    while(inputIterator.hasNext())
    {
      InstanceOperator op = (InstanceOperator) inputIterator.next();
      String opExtent = "";
      if(op instanceof InstanceExchangePart)
      {
        InstanceExchangePart exOp = (InstanceExchangePart) op;
        opExtent = exOp.getExtent();
      }
      else
      {
        opExtent = op.getExtent();
      }
      if(extent.equals(opExtent))
        this.updateCollection(operator,this.windows.get(op), allInputWindows);
    }
    return allInputWindows.get(operator);
  }
  
  public ArrayList<Window> createAcquirePacket(Long beta)
  {
    ArrayList<Window> windows = new ArrayList<Window>();
    for(int index =1; index <= beta; index++)
    {
      windows.add(new Window(1, index));
    }
    return windows;
  }
  
  public Window getWindow(int index, ArrayList<Window> windowsOfExtent)
  {
    Iterator<Window> windowIterator = windowsOfExtent.iterator();
    while(windowIterator.hasNext())
    {
      Window window = windowIterator.next();
      if(window.getWindowID() == index)
        return window;
    }
    return new Window(0,index);
  }

  public static int determineNoTuplesFromWindows(ArrayList<Window> outputWindows)
  {
    Iterator<Window> windowIterator =outputWindows.iterator();
    int tuples =0;
    while(windowIterator.hasNext())
    {
      Window window = windowIterator.next();
      tuples+= window.getTuples();
    }
    return tuples;
  }
  
  public int determineNoTuplesFromWindows(InstanceOperator extent)
  {
    Iterator<Window> windowIterator =this.windows.get(extent).iterator();
    int tuples =0;
    while(windowIterator.hasNext())
    {
      Window window = windowIterator.next();
      tuples+= window.getTuples();
    }
    return tuples;
  }

  public void clear()
  {
    this.windows = new HashMapList<InstanceOperator, Window>();
  }

  public ArrayList<Window> returnWindowsForTuples(int startCountFrom,
                                                  int numberOftuples,
                                                  InstanceOperator extent)
  {
    int counter =0;
    int tuplesCounted = 0;
    ArrayList<Window> doneWindows = new ArrayList<Window>();
    ArrayList<Window> windows = this.windows.get(extent);
    if(windows.size() == 0)
      System.out.println();
    Iterator<Window> windowIterator = windows.iterator();
    boolean done = false;
    while(windowIterator.hasNext() && !done)
    {
      Window window = windowIterator.next();
      for(int currentTuple =0; currentTuple < window.getTuples(); currentTuple++)
      {
        if(counter < startCountFrom)
          counter++;
        else
        {
          if(doneWindows.size() == 0)
          {
            Window newWindow = new Window(1, window.getWindowID());
            doneWindows.add(newWindow);
          }
          else
          {
            Window newWindow = doneWindows.get(doneWindows.size()-1);
            if(newWindow.getWindowID() == window.getWindowID())
            {
              newWindow.setUples(newWindow.getTuples() + 1);
            }
            else
            {
              newWindow = new Window(1, window.getWindowID());
              doneWindows.add(newWindow);
            }
          }
          tuplesCounted++;
        }
        if(tuplesCounted == numberOftuples)
          done = true;
      }
    }
    return doneWindows;
  }

  public ArrayList<Window> getWindowsOfExtent(InstanceOperator op)
  {
    return this.windows.get(op);
  }
  
  
  
  
}
