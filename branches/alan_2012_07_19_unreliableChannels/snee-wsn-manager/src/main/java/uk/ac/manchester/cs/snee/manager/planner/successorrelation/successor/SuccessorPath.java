package uk.ac.manchester.cs.snee.manager.planner.successorrelation.successor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;

public class SuccessorPath implements Serializable
{
  /**
   * serial version
   */
  private static final long serialVersionUID = -3640092310464288868L;
  
  
  private ArrayList<Successor> listOfSuccessors = null;
  
  public SuccessorPath(ArrayList<Successor> listOfSuccessors)
  {
    this.listOfSuccessors = new ArrayList<Successor>();
    this.listOfSuccessors.addAll(listOfSuccessors);
  }
  
  public int overallSuccessorPathLifetime()
  {
    Iterator<Successor> successorIterator = listOfSuccessors.iterator();
    int agendaCount = 0;
    while(successorIterator.hasNext())
    {
      Successor successor = successorIterator.next();
      if(successor.getAgendaCount() >= 0)
        agendaCount += successor.getAgendaCount();
      if(!successorIterator.hasNext())
        if(successor.getBasicLifetimeInAgendas() >= 0)
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

  /**
   * changes a successors time period, requires a recalculation of lifetime.
   * @param newSuccessorTimeSwitch
   * @param runningSites 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws SNEEConfigurationException 
   */
  public void adjustSuccessorSwitchTime(int newSuccessorTimeSwitch, int successorIndex,
                                        HashMap<String, RunTimeSite> runningSites) 
  throws OptimizationException, SchemaMetadataException,
  TypeMappingException, SNEEConfigurationException
  {
    this.listOfSuccessors.get(successorIndex).setAgendaCount(newSuccessorTimeSwitch);
    recaulcateLifetime(runningSites);
  }

  /**
   * takes the energy measurements from the first successor and determines final lifetime.
   * @param runningSites 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws SNEEConfigurationException 
   */
  private void recaulcateLifetime(HashMap<String, RunTimeSite> runningSites) 
  throws OptimizationException, SchemaMetadataException,
  TypeMappingException, SNEEConfigurationException
  {
    Iterator<Successor> pathSuccessors = this.listOfSuccessors.iterator();
    while(pathSuccessors.hasNext())
    {
      Successor currentSuccessor = pathSuccessors.next();
  //   if(pathSuccessors.hasNext() == false)
    //  {
      //  System.out.println("");
      //}
      runningSites = currentSuccessor.recalculateRunningSitesCosts(runningSites);
    }
  }
}
