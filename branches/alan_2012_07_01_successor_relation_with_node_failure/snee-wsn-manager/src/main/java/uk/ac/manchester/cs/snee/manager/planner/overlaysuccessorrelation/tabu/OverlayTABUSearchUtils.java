package uk.ac.manchester.cs.snee.manager.planner.overlaysuccessorrelation.tabu;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import uk.ac.manchester.cs.snee.manager.planner.common.OverlaySuccessor;
import uk.ac.manchester.cs.snee.manager.planner.common.OverlaySuccessorPath;

public class OverlayTABUSearchUtils
{
  private BufferedWriter out = null;
  private BufferedWriter TABUout = null;
  private String sep = System.getProperty("file.separator");
  
  public OverlayTABUSearchUtils(String TABUOutputFolder) throws IOException
  {
    this.out = new BufferedWriter(new FileWriter(TABUOutputFolder + sep + "log"));
    this.TABUout = new BufferedWriter(new FileWriter(TABUOutputFolder + sep + "TABUlog"));
    
    out.write("iteration \t nextSuccessorName \t nextSuccessorLifetime \t previous lifetime \t positionInPath \n");
    out.flush();
    
  }
  
  public void writeNewSuccessor(OverlaySuccessor successor, int iteration, OverlaySuccessor preivousBest, 
                                OverlaySuccessorPath currentPath) 
  throws IOException
  {
    out.write(iteration + " : " + successor.toString() + " : " + 
              successor.getLifetimeInAgendas() + " : " + preivousBest.getLifetimeInAgendas() + " : " +
              (currentPath.successorLength() -1) + "\n");
    out.flush();
  }

  public void writeFailedSuccessor(OverlaySuccessor bestNeighbourHoodSuccessor,
      int iteration, OverlaySuccessor currentBestSuccessor, OverlaySuccessorPath currentPath) 
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
  public void outputTABUList(int iteration, OverlayTABUList list) 
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

  public void writeNoSuccessor(int iteration) 
  throws IOException
  {
    out.write(iteration + " : No Neighbourhood generated therefore no Successor Found"  + "\n");
    out.flush();
    
  }
  
}
