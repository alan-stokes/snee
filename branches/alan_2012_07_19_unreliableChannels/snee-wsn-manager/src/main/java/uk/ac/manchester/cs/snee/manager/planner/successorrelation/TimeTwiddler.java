package uk.ac.manchester.cs.snee.manager.planner.successorrelation;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.WhenSchedulerException;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.planner.successorrelation.successor.Successor;
import uk.ac.manchester.cs.snee.manager.planner.successorrelation.successor.SuccessorPath;
import uk.ac.manchester.cs.snee.manager.planner.successorrelation.tabu.TabuSearch;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;

import com.rits.cloning.Cloner;

public class TimeTwiddler
{
  
  public static int bestFoundLifetime;
  public static SuccessorPath bestPath;
  public static HashMap<String, RunTimeSite> runningSites;
  public static Cloner cloner = new Cloner();
  public static BufferedWriter out;
  /**
   * tweaks the times of the path to see how well tuned the final path is.
   * @param search 
   * @param out 
   * @param runningSites 
   * @param bestSuccessorRelation
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws WhenSchedulerException 
   * @throws SNEEException 
   * @throws SNEEConfigurationException 
   * @throws CodeGenerationException 
   * @throws IOException 
   * @throws NumberFormatException 
   */
  public static SuccessorPath adjustTimesTest(SuccessorPath original,
                                              HashMap<String, RunTimeSite> originalRunningSites,
                                              boolean WithRecompute, TabuSearch search,
                                              BufferedWriter outWriter)
  throws OptimizationException, SchemaMetadataException, TypeMappingException,
  NumberFormatException, IOException, CodeGenerationException, SNEEConfigurationException, 
  SNEEException, WhenSchedulerException
  {
    if(WithRecompute)
      System.out.println("starting twiddle time tests with recompution though genetics");
    else
      System.out.println("starting twiddle time tests with no recompution though genetics");
    cloner.dontClone(Logger.class);
    out = outWriter;
    SuccessorPath usableCopy = cloner.deepClone(original);
    int numberofAgendasToJumpBetween = 1000;
    int overallLifetime = original.overallSuccessorPathLifetime();
    bestFoundLifetime = overallLifetime;
    bestPath = original;
    runningSites = originalRunningSites;
    //clear agenda count of last successor (being not correct)
    usableCopy.getSuccessorList().get(usableCopy.successorLength()).setAgendaCount(0);
    
    //iterate though successors adjusting times
    ArrayList<Successor> successors = usableCopy.getSuccessorList();
    for(int successorIndex = 0; successorIndex < successors.size(); successorIndex ++)
    {
      testSuccessor(successors, successorIndex, numberofAgendasToJumpBetween, usableCopy,
                    overallLifetime, WithRecompute, search);
    }
    
    System.out.println("finished twiddle.");
    System.out.println("best successor time is " + bestFoundLifetime);
    return bestPath;
  }

  private static void testSuccessor(ArrayList<Successor> successors, int successorIndex,
                                    int numberofAgendasToJumpBetween, SuccessorPath usableCopy,
                                    int overallLifetime, boolean WithRecompute,
                                    TabuSearch search)
  throws OptimizationException, SchemaMetadataException, TypeMappingException,
  NumberFormatException, IOException, CodeGenerationException,
  SNEEConfigurationException, SNEEException, WhenSchedulerException
  {
    Successor currentSuccessor = successors.get(successorIndex);
    int currentSuccessorTimeSwitch = currentSuccessor.getAgendaCount();
    int newSuccessorTimeSwitch =  currentSuccessorTimeSwitch;
    while(newSuccessorTimeSwitch > 0)
    {
      newSuccessorTimeSwitch = newSuccessorTimeSwitch - numberofAgendasToJumpBetween;
      usableCopy.adjustSuccessorSwitchTime(newSuccessorTimeSwitch, successorIndex, runningSites);
      int newlifetime = usableCopy.overallSuccessorPathLifetime();
      if(newlifetime > bestFoundLifetime)
      {
        bestFoundLifetime = newlifetime;
        bestPath = cloner.deepClone(usableCopy);
      }
      int finalPlanLifetime = 
        usableCopy.getSuccessorList().get(usableCopy.successorLength()).calculateLifetime();
      if(WithRecompute && finalPlanLifetime > 0)
      {
        SuccessorPath path = search.findSuccessorsPath(cloner.deepClone(usableCopy));
        newlifetime = path.overallSuccessorPathLifetime();
        if(newlifetime > bestFoundLifetime)
        {
          bestFoundLifetime = newlifetime;
          bestPath = cloner.deepClone(path);
          System.out.println("new best plan has a lifetime of " + bestFoundLifetime);
        }
      }
      writeSuccessorsTimes(usableCopy);
      adjustTimesTestRecursive(successors, successorIndex+1, usableCopy, 
                               numberofAgendasToJumpBetween, overallLifetime, WithRecompute, search);
    
    }
    newSuccessorTimeSwitch = currentSuccessorTimeSwitch;
    
    while(newSuccessorTimeSwitch < overallLifetime)
    {
      newSuccessorTimeSwitch = newSuccessorTimeSwitch + numberofAgendasToJumpBetween;
      usableCopy.adjustSuccessorSwitchTime(newSuccessorTimeSwitch, successorIndex, runningSites);
      int newlifetime = usableCopy.overallSuccessorPathLifetime();
      if(newlifetime > bestFoundLifetime)
      {
        bestFoundLifetime = newlifetime;
        bestPath = cloner.deepClone(usableCopy);
      }
      int finalPlanLifetime = 
        usableCopy.getSuccessorList().get(usableCopy.successorLength()).calculateLifetime();
      if(WithRecompute && finalPlanLifetime > 0)
      {
        SuccessorPath path = search.findSuccessorsPath(cloner.deepClone(usableCopy));
        newlifetime = path.overallSuccessorPathLifetime();
        if(newlifetime > bestFoundLifetime)
        {
          bestFoundLifetime = newlifetime;
          bestPath = cloner.deepClone(path);
          System.out.println("new best plan has a lifetime of " + bestFoundLifetime);
        }
      }
      writeSuccessorsTimes(usableCopy);
      adjustTimesTestRecursive(successors, successorIndex+1, usableCopy, 
                               numberofAgendasToJumpBetween, overallLifetime, WithRecompute, search);
    }
  }

  private static void writeSuccessorsTimes(SuccessorPath usableCopy) throws IOException
  {
    Iterator<Successor> successorIterator = usableCopy.getSuccessorList().iterator();
    String output = "";
    int counter = 0;
    if(usableCopy.overallSuccessorPathLifetime() > 0)
    {
      while(successorIterator.hasNext())
      {
        Successor nextSuccessor = successorIterator.next();
        output = output.concat("successor " + counter + " time " + nextSuccessor.getAgendaCount());
        counter ++;
      }
      output = output.concat(" lifetime " + usableCopy.overallSuccessorPathLifetime());
      out.write(output + "\n");
      out.flush();
    }
  }

  private static void adjustTimesTestRecursive(ArrayList<Successor> successors,
                                               int newSuccessorIndex, 
                                               SuccessorPath usableCopy,
                                               int numberofAgendasToJumpBetween,
                                               int overallLifetime,
                                               boolean WithRecompute,
                                               TabuSearch search)
  throws OptimizationException, SchemaMetadataException, TypeMappingException,
  NumberFormatException, IOException, CodeGenerationException, SNEEConfigurationException, 
  SNEEException, WhenSchedulerException
  {
    for(int successorIndex = newSuccessorIndex; successorIndex < successors.size(); successorIndex ++)
    {
      testSuccessor(successors, successorIndex, numberofAgendasToJumpBetween, usableCopy,
                    overallLifetime, WithRecompute, search);
    }
  }
}
