package uk.ac.manchester.cs.snee.manager.planner.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;

public class OverlaySuccessorPath implements Serializable
{
  /**
   * serial version
   */
  private static final long serialVersionUID = -3640092310464288868L;
  
  
  private ArrayList<OverlaySuccessor> listOfSuccessors = null;
  
  public OverlaySuccessorPath(ArrayList<OverlaySuccessor> listOfSuccessors)
  {
    this.listOfSuccessors = new ArrayList<OverlaySuccessor>();
    this.listOfSuccessors.addAll(listOfSuccessors);
  }
  
  public int overallAgendaLifetime()
  {
    Iterator<OverlaySuccessor> successorIterator = listOfSuccessors.iterator();
    int agendaCount = 0;
    while(successorIterator.hasNext())
    {
      OverlaySuccessor successor = successorIterator.next();
      agendaCount += successor.getEstimatedLifetimeInAgendaCountBeforeSwitch();
      if(!successorIterator.hasNext())
        agendaCount += successor.getBasicLifetimeInAgendas();
    }
    return agendaCount;
  }
  
  public void updateList(ArrayList<OverlaySuccessor> listOfSuccessors)
  {
    clearPath();
    this.listOfSuccessors.addAll(listOfSuccessors);
  }
  
  public ArrayList<OverlaySuccessor> getSuccessorList()
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

  public void add(OverlaySuccessor nextSuccessor)
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
   */
  public void adjustSuccessorSwitchTime(int newSuccessorTimeSwitch, int successorIndex,
                                        HashMap<String, RunTimeSite> runningSites) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException
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
   */
  private void recaulcateLifetime(HashMap<String, RunTimeSite> runningSites) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    Iterator<OverlaySuccessor> pathSuccessors = this.listOfSuccessors.iterator();
    while(pathSuccessors.hasNext())
    {
    	OverlaySuccessor currentSuccessor = pathSuccessors.next();
  //   if(pathSuccessors.hasNext() == false)
    //  {
      //  System.out.println("");
      //}
      runningSites = currentSuccessor.recalculateRunningSitesCosts(runningSites);
    }
  }
}
