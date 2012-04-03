package uk.ac.manchester.cs.snee.manager.planner.common;

import java.util.ArrayList;
import java.util.Iterator;

public class SuccessorPath
{
  private ArrayList<Successor> listOfSuccessors = null;
  
  public SuccessorPath(ArrayList<Successor> listOfSuccessors)
  {
    this.listOfSuccessors = new ArrayList<Successor>();
    this.listOfSuccessors.addAll(listOfSuccessors);
  }
  
  public int overallAgendaLifetime()
  {
    Iterator<Successor> successorIterator = listOfSuccessors.iterator();
    int agendaCount = 0;
    while(successorIterator.hasNext())
    {
      Successor successor = successorIterator.next();
      agendaCount += successor.getAgendaCount();
      if(!successorIterator.hasNext())
        agendaCount += successor.getBasicLifetimeInAgendas();
    }
    return agendaCount;
  }
  
  public void updateList(ArrayList<Successor> listOfSuccessors)
  {
    clearPath();
    this.listOfSuccessors.addAll(listOfSuccessors);
  }
  
  public ArrayList<Successor> getSuccessorList()
  {
    return this.listOfSuccessors;
  }
  
  public int successorLength()
  {
    return listOfSuccessors.size();
  }
  
  public void removeSuccessor(int position)
  {
    this.listOfSuccessors.remove(position);
  }

  public void add(Successor nextSuccessor)
  {
    this.listOfSuccessors.add(nextSuccessor);
  }

  public void clearPath()
  {
    this.listOfSuccessors.clear();
  }
}
