package uk.ac.manchester.cs.snee.manager.planner.overlaysuccessorrelation.tabu;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.planner.common.OverlaySuccessor;
import uk.ac.manchester.cs.snee.manager.planner.common.OverlaySuccessorPath;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;

public class OverlayTABUList
{
  private HashMap<Integer, Set<OverlayTABUSuccessor>> TABUList;
  private int TABUTenure = 5;
  
  public OverlayTABUList ()
  {
    TABUList = new HashMap<Integer, Set<OverlayTABUSuccessor>>();
  }
  
  public void addToTABUList(OverlaySuccessor successor, int position, boolean entireity)
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    Set<OverlayTABUSuccessor> diversificationTABUList = new HashSet<OverlayTABUSuccessor>();
    //get correct tabuList
    if(TABUList.get(position) != null)
    {
      diversificationTABUList = TABUList.get(position); 
    }
    
    if(this.contains(diversificationTABUList, successor.getQep()))
    {
      OverlayTABUSuccessor tabuedSuccesor = this.getTABUSuccessor(diversificationTABUList, successor.getQep());
      if(entireity)
        tabuedSuccesor.setEntirelyTABUed(entireity);
      else
        tabuedSuccesor.addTimesTABUed(successor.getEstimatedLifetimeInAgendaCountBeforeSwitch());
      diversificationTABUList.remove(tabuedSuccesor);
      diversificationTABUList.add(tabuedSuccesor);
    }
    else
    {
      if(entireity)
      {
        OverlayTABUSuccessor tabued = new OverlayTABUSuccessor(successor.getQep(), successor.getNewRunTimeSites(), true);
        diversificationTABUList.add(tabued);
      }
      else
      {
        ArrayList<Integer> times = new ArrayList<Integer>();
        times.add(successor.getEstimatedLifetimeInAgendaCountBeforeSwitch());
        diversificationTABUList.add(new OverlayTABUSuccessor(successor.getQep(), successor.getNewRunTimeSites(),
                                                      times));
      }
    }
        
    //remove any off TABUTenure
    while(diversificationTABUList.size() > TABUTenure)
    {
      OverlayTABUSuccessor successorToRemove = diversificationTABUList.iterator().next();
      diversificationTABUList.remove(successorToRemove);
    }
    //restore
    TABUList.put(position, diversificationTABUList); 
  }
  
  
  
  private boolean contains(Set<OverlayTABUSuccessor> diversificationTABUList,
                           SensorNetworkQueryPlan plan)
  {
    Iterator<OverlayTABUSuccessor> tabuList = diversificationTABUList.iterator();
    while(tabuList.hasNext())
    {
      OverlayTABUSuccessor tabu = tabuList.next();
      if(tabu.getFormat().equals(plan.getIOT().getStringForm()))
        return true;
    }
    return false;
  }
  
  private OverlayTABUSuccessor getTABUSuccessor(Set<OverlayTABUSuccessor> diversificationTABUList,
                                         SensorNetworkQueryPlan plan)
  {
    Iterator<OverlayTABUSuccessor> tabuIterator = diversificationTABUList.iterator();
    while(tabuIterator.hasNext())
    {
      OverlayTABUSuccessor tabu= tabuIterator.next();
      if(tabu.equals(plan))
        return tabu;
    }
    return null;
  }

  public void addAllPathIntoTABUList(OverlaySuccessorPath path, int position, OverlaySuccessor initialSuccessor) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    Iterator<OverlaySuccessor> successorIterator = path.getSuccessorList().iterator();
    Set<OverlayTABUSuccessor> diversificationTABUList = new HashSet<OverlayTABUSuccessor>();
    if(TABUList.get(position) != null)
    {
      diversificationTABUList = TABUList.get(position); 
    }
    while(successorIterator.hasNext())
    {
      OverlaySuccessor pathSuccessor = successorIterator.next();
      if(!pathSuccessor.toString().equals(initialSuccessor.toString()))
        diversificationTABUList.add(new OverlayTABUSuccessor(pathSuccessor.getQep(), 
                                                      pathSuccessor.getNewRunTimeSites(), 
                                                      true));
    }
    //remove any off TABUTenure
    while(diversificationTABUList.size() > TABUTenure)
    {
      OverlayTABUSuccessor removal = diversificationTABUList.iterator().next();
      diversificationTABUList.remove(removal);
    }
    //restore
    TABUList.put(position, diversificationTABUList); 
  }
  
  public boolean isTabu(SensorNetworkQueryPlan plan, int position)
  {
    Set<OverlayTABUSuccessor> diversificationTABUList = new HashSet<OverlayTABUSuccessor>();
    if(TABUList.get(position) != null)
    {
      diversificationTABUList = TABUList.get(position); 
      return this.contains(diversificationTABUList, plan);
    }
    else
      return false; 
  }
  
  public boolean isEntirelyTABU(SensorNetworkQueryPlan plan, int position)
  {
    if(this.isTabu(plan, position))
    {
      Set<OverlayTABUSuccessor> diversificationTABUList = new HashSet<OverlayTABUSuccessor>();
      diversificationTABUList = TABUList.get(position); 
      OverlayTABUSuccessor tabued = this.getTABUSuccessor(diversificationTABUList, plan);
      return tabued.isEntirelyTABUed();
    }
    else
      return false;
  }
  
  public ArrayList<Integer> getTABUTimes(SensorNetworkQueryPlan plan, int position)
  {
    if(this.isTabu(plan, position))
    {
      Set<OverlayTABUSuccessor> diversificationTABUList = new HashSet<OverlayTABUSuccessor>();
      diversificationTABUList = TABUList.get(position); 
      OverlayTABUSuccessor tabued = this.getTABUSuccessor(diversificationTABUList, plan);
      return tabued.getTimesTABUed();
    }
    else
      return new ArrayList<Integer>();
  }
  
  /**
   * checks if the new successor meets any of the criteria for TABU. 
   * (currently this means if the successor is in the TABU list.
   * but could easily be attributed).
   * @param successor
   * @param currentPath 
   * @return
   */
  public boolean meetsTABUCriteria(OverlaySuccessor successor, OverlaySuccessorPath currentPath)
  {
    //locates the diversification TABU list for long term memory
    Set<OverlayTABUSuccessor> DiversificationTABUList = new HashSet<OverlayTABUSuccessor>();
    if(currentPath != null)
    {
      if(TABUList.get(currentPath.successorLength() -1) != null)
      {
        DiversificationTABUList = TABUList.get(currentPath.successorLength() -1);
      }   
    }
    Iterator<OverlayTABUSuccessor> tabuIterator = DiversificationTABUList.iterator();
    while(tabuIterator.hasNext())
    {
      OverlayTABUSuccessor tabued = tabuIterator.next();
      if(tabued.toString().equals(successor.toString()))
        return true;
    }
    return false;
  }
  
  /**
   * checks the successor to see if it gives a large benefit (at which point will allow out of 
   * the TABU list)
   * 
   * @param successor
   * @return
   */
  public boolean passesAspirationCriteria(OverlaySuccessor successor, OverlaySuccessor bestCurrentSuccessor)
  {
    if(successor.getEstimatedLifetimeInAgendas() > bestCurrentSuccessor.getEstimatedLifetimeInAgendas())
      return true;
    else
      return false;
  }
  
  
  /**
   * used to restart the search 
   * @param neighbourHood
   * @param currentBestSuccessor
   * @param current path
   * @throws IOException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  public OverlaySuccessor engageDiversificationTechnique(
		  ArrayList<OverlaySuccessor> neighbourHood, 
		  OverlaySuccessor currentBestSuccessor,
		  OverlaySuccessorPath currentPath, int iteration,
          OverlayTABUSearchUtils utils) 
  throws IOException, OptimizationException, SchemaMetadataException, TypeMappingException
  { 
    //update TABUList
    int length = currentPath.successorLength() -1;
    Random random = new Random(new Long(0));
    //int positionToMoveTo = length -1;
    int positionToMoveTo = 0;
    if(length == 0)
    {
      utils.outputNODiversification(iteration);
    }
    else
    {
      if(length == 1)
      {
        positionToMoveTo = 0;
      }
      else
      {
        positionToMoveTo = random.nextInt(length -1);
      }
      
      if(positionToMoveTo >= 0)
      {
        for(int position = currentPath.successorLength() -1; position > positionToMoveTo; position--)
        {
          Set<OverlayTABUSuccessor> DiverseTABUList = TABUList.get(position);
          if(DiverseTABUList == null)
          {
            DiverseTABUList = new HashSet<OverlayTABUSuccessor>();
          }
          OverlaySuccessor pathSuccessor = currentPath.getSuccessorList().get(position);
          DiverseTABUList.add(new OverlayTABUSuccessor(pathSuccessor.getQep(), pathSuccessor.getNewRunTimeSites(), 
                                                new ArrayList<Integer>(pathSuccessor.getEstimatedLifetimeInAgendaCountBeforeSwitch())));
          TABUList.put(position, DiverseTABUList);
         
          currentPath.removeSuccessor(position);
          currentBestSuccessor = currentPath.getSuccessorList().get(currentPath.getSuccessorList().size() -1);
          
          //remove all tabuList after the next position
          Iterator<Integer> keyIterator = TABUList.keySet().iterator();
          ArrayList<Integer> keysToRemove = new ArrayList<Integer>();
          while(keyIterator.hasNext())
          {
            Integer key = keyIterator.next();
            if(key > positionToMoveTo)
              keysToRemove.add(key);
          }
          keyIterator = keysToRemove.iterator();
          while(keyIterator.hasNext())
          {
            Integer key = keyIterator.next();
            TABUList.remove(key);
          }
        }
      } 
      utils.outputDiversification(iteration, positionToMoveTo);
      return currentBestSuccessor;
    }
    return currentBestSuccessor;
  }
  
  public String toString()
  {
    String output = "";
    Iterator<Integer> keyIterator = this.TABUList.keySet().iterator();
    while(keyIterator.hasNext())
    {
      Integer key = keyIterator.next();
      output = output.concat("position " + key + "\n");
      Set<OverlayTABUSuccessor> TABUlistAtKey = this.TABUList.get(key);
      Iterator<OverlayTABUSuccessor> TABUedSuccessors = TABUlistAtKey.iterator();
      while(TABUedSuccessors.hasNext())
      {
        OverlayTABUSuccessor tabuedSuccessor = TABUedSuccessors.next();
        output = output.concat(tabuedSuccessor.getID() + " AT " + tabuedSuccessor.getTimes() + "\n");
      }
    }
    
    output = output.concat("\n\n*****************************************\n\n");
    return output;
  }
  
  
}
