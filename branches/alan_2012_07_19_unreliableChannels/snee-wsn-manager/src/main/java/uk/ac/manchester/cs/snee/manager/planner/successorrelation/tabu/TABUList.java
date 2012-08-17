package uk.ac.manchester.cs.snee.manager.planner.successorrelation.tabu;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.planner.successorrelation.successor.Successor;
import uk.ac.manchester.cs.snee.manager.planner.successorrelation.successor.SuccessorPath;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;

public class TABUList
{
  private HashMap<Integer, Set<TABUSuccessor>> TABUList;
  private int TABUTenure = 5;
  
  public TABUList ()
  {
    TABUList = new HashMap<Integer, Set<TABUSuccessor>>();
  }
  
  public void addToTABUList(Successor successor, int position, boolean entireity)
  throws OptimizationException, SchemaMetadataException,
  TypeMappingException, SNEEConfigurationException
  {
    Set<TABUSuccessor> diversificationTABUList = new HashSet<TABUSuccessor>();
    //get correct tabuList
    if(TABUList.get(position) != null)
    {
      diversificationTABUList = TABUList.get(position); 
    }
    
    if(this.contains(diversificationTABUList, successor.getQep()))
    {
      TABUSuccessor tabuedSuccesor = this.getTABUSuccessor(diversificationTABUList, successor.getQep());
      if(entireity)
        tabuedSuccesor.setEntirelyTABUed(entireity);
      else
        tabuedSuccesor.addTimesTABUed(successor.getAgendaCount());
      diversificationTABUList.remove(tabuedSuccesor);
      diversificationTABUList.add(tabuedSuccesor);
    }
    else
    {
      if(entireity)
      {
        TABUSuccessor tabued = new TABUSuccessor(successor.getQep(), successor.getNewRunTimeSites(), true);
        diversificationTABUList.add(tabued);
      }
      else
      {
        ArrayList<Integer> times = new ArrayList<Integer>();
        times.add(successor.getAgendaCount());
        diversificationTABUList.add(new TABUSuccessor(successor.getQep(), successor.getNewRunTimeSites(),
                                                      times));
      }
    }
        
    //remove any off TABUTenure
    while(diversificationTABUList.size() > TABUTenure)
    {
      TABUSuccessor successorToRemove = diversificationTABUList.iterator().next();
      diversificationTABUList.remove(successorToRemove);
    }
    //restore
    TABUList.put(position, diversificationTABUList); 
  }
  
  
  
  private boolean contains(Set<TABUSuccessor> diversificationTABUList,
                           SensorNetworkQueryPlan plan)
  {
    Iterator<TABUSuccessor> tabuList = diversificationTABUList.iterator();
    while(tabuList.hasNext())
    {
      TABUSuccessor tabu = tabuList.next();
      if(tabu.getFormat().equals(plan.getIOT().getStringForm()))
        return true;
    }
    return false;
  }
  
  private TABUSuccessor getTABUSuccessor(Set<TABUSuccessor> diversificationTABUList,
                                         SensorNetworkQueryPlan plan)
  {
    Iterator<TABUSuccessor> tabuIterator = diversificationTABUList.iterator();
    while(tabuIterator.hasNext())
    {
      TABUSuccessor tabu= tabuIterator.next();
      if(tabu.equals(plan))
        return tabu;
    }
    return null;
  }

  public void addAllPathIntoTABUList(SuccessorPath path, int position, Successor initialSuccessor) 
  throws OptimizationException, SchemaMetadataException,
  TypeMappingException, SNEEConfigurationException
  {
    Iterator<Successor> successorIterator = path.getSuccessorList().iterator();
    Set<TABUSuccessor> diversificationTABUList = new HashSet<TABUSuccessor>();
    if(TABUList.get(position) != null)
    {
      diversificationTABUList = TABUList.get(position); 
    }
    while(successorIterator.hasNext())
    {
      Successor pathSuccessor = successorIterator.next();
      if(!pathSuccessor.toString().equals(initialSuccessor.toString()))
        diversificationTABUList.add(new TABUSuccessor(pathSuccessor.getQep(), 
                                                      pathSuccessor.getNewRunTimeSites(), 
                                                      true));
    }
    //remove any off TABUTenure
    while(diversificationTABUList.size() > TABUTenure)
    {
      TABUSuccessor removal = diversificationTABUList.iterator().next();
      diversificationTABUList.remove(removal);
    }
    //restore
    TABUList.put(position, diversificationTABUList); 
  }
  
  public boolean isTabu(SensorNetworkQueryPlan plan, int position)
  {
    Set<TABUSuccessor> diversificationTABUList = new HashSet<TABUSuccessor>();
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
      Set<TABUSuccessor> diversificationTABUList = new HashSet<TABUSuccessor>();
      diversificationTABUList = TABUList.get(position); 
      TABUSuccessor tabued = this.getTABUSuccessor(diversificationTABUList, plan);
      return tabued.isEntirelyTABUed();
    }
    else
      return false;
  }
  
  public ArrayList<Integer> getTABUTimes(SensorNetworkQueryPlan plan, int position)
  {
    if(this.isTabu(plan, position))
    {
      Set<TABUSuccessor> diversificationTABUList = new HashSet<TABUSuccessor>();
      diversificationTABUList = TABUList.get(position); 
      TABUSuccessor tabued = this.getTABUSuccessor(diversificationTABUList, plan);
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
  public boolean meetsTABUCriteria(Successor successor, SuccessorPath currentPath)
  {
    //locates the diversification TABU list for long term memory
    Set<TABUSuccessor> DiversificationTABUList = new HashSet<TABUSuccessor>();
    if(currentPath != null)
    {
      if(TABUList.get(currentPath.successorLength() -1) != null)
      {
        DiversificationTABUList = TABUList.get(currentPath.successorLength() -1);
      }   
    }
    Iterator<TABUSuccessor> tabuIterator = DiversificationTABUList.iterator();
    while(tabuIterator.hasNext())
    {
      TABUSuccessor tabued = tabuIterator.next();
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
  public boolean passesAspirationCriteria(Successor successor, Successor bestCurrentSuccessor)
  {
    if(successor.getLifetimeInAgendas() > bestCurrentSuccessor.getLifetimeInAgendas())
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
   * @throws SNEEConfigurationException 
   */
  public Successor engageDiversificationTechnique(ArrayList<Successor> neighbourHood, 
                                                  Successor currentBestSuccessor,
                                                  SuccessorPath currentPath, int iteration,
                                                  TABUSearchUtils utils) 
  throws IOException, OptimizationException, SchemaMetadataException,
  TypeMappingException, SNEEConfigurationException
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
          Set<TABUSuccessor> DiverseTABUList = TABUList.get(position);
          if(DiverseTABUList == null)
          {
            DiverseTABUList = new HashSet<TABUSuccessor>();
          }
          Successor pathSuccessor = currentPath.getSuccessorList().get(position);
          DiverseTABUList.add(new TABUSuccessor(pathSuccessor.getQep(), pathSuccessor.getNewRunTimeSites(), 
                                                new ArrayList<Integer>(pathSuccessor.getAgendaCount())));
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
      Set<TABUSuccessor> TABUlistAtKey = this.TABUList.get(key);
      Iterator<TABUSuccessor> TABUedSuccessors = TABUlistAtKey.iterator();
      while(TABUedSuccessors.hasNext())
      {
        TABUSuccessor tabuedSuccessor = TABUedSuccessors.next();
        output = output.concat(tabuedSuccessor.getID() + " AT " + tabuedSuccessor.getTimes() + "\n");
      }
    }
    
    output = output.concat("\n\n*****************************************\n\n");
    return output;
  }
  
  
}
