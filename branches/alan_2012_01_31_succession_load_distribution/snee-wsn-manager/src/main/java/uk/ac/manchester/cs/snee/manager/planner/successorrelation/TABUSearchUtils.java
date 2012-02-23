package uk.ac.manchester.cs.snee.manager.planner.successorrelation;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class TABUSearchUtils
{
  private BufferedWriter out = null;
  private BufferedWriter TABUout = null;
  private String sep = System.getProperty("file.separator");
  
  public TABUSearchUtils(String TABUOutputFolder) throws IOException
  {
    this.out = new BufferedWriter(new FileWriter(TABUOutputFolder + sep + "log"));
    this.TABUout = new BufferedWriter(new FileWriter(TABUOutputFolder + sep + "TABUlog"));
    
    out.write("iteration \t nextSuccessorName \t nextSuccessorLifetime \t previous lifetime \t positionInPath \n");
    out.flush();
    
  }
  
  public void writeNewSuccessor(Successor successor, int iteration, Successor preivousBest, 
                                SuccessorPath currentPath) 
  throws IOException
  {
    out.write(iteration + " : " + successor.toString() + " : " + 
              successor.getLifetimeInAgendas() + " : " + preivousBest.getLifetimeInAgendas() + " : " +
              (currentPath.successorLength() -1) + "\n");
    out.flush();
  }

  public void writeFailedSuccessor(Successor bestNeighbourHoodSuccessor,
      int iteration, Successor currentBestSuccessor, SuccessorPath currentPath) 
  throws IOException
  {
    out.write(iteration + " : BEST OPTION FAILED (" + bestNeighbourHoodSuccessor.toString() + " : " + 
        bestNeighbourHoodSuccessor.getLifetimeInAgendas() + " : " + 
        currentBestSuccessor.getLifetimeInAgendas() + " ) " + "\n");
    out.flush();
  }

  /**
   * outputs the current TABU into the TABU out.
   * @param iteration 
   * @throws IOException 
   */
  public void outputTABUList(int iteration, TABUList list) 
  throws IOException
  {
    TABUout.write(iteration + "\n");
    String output = list.toString();
    TABUout.write(output + "\n");
    TABUout.flush();
  }
  
  public void outputDiversification(int iteration, int positionToMoveTo) 
  throws IOException
  {
    out.write(iteration + " : ENGADED DIVERSIFICATION TECHNIQUE: moving to Position " + positionToMoveTo + "\n");
    out.flush();
  }

  public void close() 
  throws IOException
  {
    out.close();
    TABUout.close();
    
  }

  public void outputNODiversification(int iteration) 
  throws IOException
  {
    out.write(iteration + " : NO DIVERSIFICATION TECHNIQUE: Already at beginning \n");
    out.flush();
  }
  
}
