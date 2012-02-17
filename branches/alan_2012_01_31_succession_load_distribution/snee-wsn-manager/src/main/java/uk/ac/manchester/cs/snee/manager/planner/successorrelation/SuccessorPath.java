package uk.ac.manchester.cs.snee.manager.planner.successorrelation;

import java.util.ArrayList;
import java.util.Iterator;

public class SuccessorPath
{
  private ArrayList<Successor> listOfSuccessors = null;
  
  public SuccessorPath(ArrayList<Successor> listOfSuccessors)
  {
    this.listOfSuccessors = listOfSuccessors;
  }
  
  public int overallAgendaLifetime()
  {
    Iterator<Successor> successorIterator = listOfSuccessors.iterator();
    int agendaCount = 0;
    while(successorIterator.hasNext())
    {
      Successor successor = successorIterator.next();
      agendaCount = agendaCount + successor.getAgendaCount();
    }
    return agendaCount;
  }
  
  public ArrayList<Successor> getSuccessorList()
  {
    return this.listOfSuccessors;
  }
}
