package uk.ac.manchester.cs.snee.manager.planner.successorrelation;

/*
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
*/
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.HashMap;

import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.AutonomicManagerComponent;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.planner.successorrelation.successor.SuccessorPath;
import uk.ac.manchester.cs.snee.manager.planner.successorrelation.tabu.TabuSearch;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;

public class SuccessorRelationManager extends AutonomicManagerComponent
{
  private static final long serialVersionUID = 891621010871659913L;
  private File successorFolder = null;
  private HashMap<String, RunTimeSite> runningSites;
  private MetadataManager _metadataManager;
  
  public SuccessorRelationManager(File plannerFolder, HashMap<String, RunTimeSite> runningSites,
                           MetadataManager _metadataManager, SourceMetadataAbstract _metadata, AutonomicManagerImpl manager)
  {
    this.manager = manager;
    this._metadata = _metadata;
    successorFolder = new File(plannerFolder.toString() + sep + "successorRelation");
    if(successorFolder.exists())
    {
      manager.deleteFileContents(successorFolder);
      successorFolder.mkdir();
    }
    else
    {
      successorFolder.mkdir();
    }
    this.runningSites = runningSites;
    this._metadataManager = _metadataManager;
  }
  
  public SuccessorPath executeSuccessorRelation(SensorNetworkQueryPlan initialPoint)
  {
    try
    {
      TabuSearch search = null;
      //set up TABU folder
      File TABUFolder = new File(successorFolder.toString() + sep + "TABU");
      if(TABUFolder.exists())
      {
        manager.deleteFileContents(TABUFolder);
        TABUFolder.mkdir();
      }
      else
      {
        TABUFolder.mkdir();
      }
      //search though space
      search = new TabuSearch(manager, runningSites, _metadata, _metadataManager, TABUFolder);
      SuccessorPath bestSuccessorRelation = search.findSuccessorsPath(initialPoint);
      new SuccessorRelationManagerUtils(this.manager, successorFolder).writeSuccessorToFile(bestSuccessorRelation.getSuccessorList(), "finalSolution");
      
     writeSuccessorPathToFile(bestSuccessorRelation);
      //SuccessorPath bestSuccessorRelation = readInSuccessor();
      //new PlannerUtils(successorFolder, this.manager).writeSuccessorToFile(bestSuccessorRelation.getSuccessorList(), "finalSolution");
     
      //added code to see how well tuned the plan is without recomputing
        //BufferedWriter out = new BufferedWriter(new FileWriter(new File(successorFolder.toString() + sep + "records")));
        //SuccessorPath twiddleBestSuccessorRelation = TimeTwiddler.adjustTimesTest(bestSuccessorRelation, runningSites, false, search, out);      
       //new PlannerUtils(successorFolder, this.manager).writeSuccessorToFile(twiddleBestSuccessorRelation.getSuccessorList(), "finalTwiddleSolution");
        //out.close();
        //added code to see how well tuned successor is by adjusting and then running entire system
   //   SuccessorPath twiddleBestSuccessorRelation = TimeTwiddler.adjustTimesTest(bestSuccessorRelation, runningSites, true, search);      
     // new PlannerUtils(successorFolder, this.manager).writeSuccessorToFile(twiddleBestSuccessorRelation.getSuccessorList(), "finalTwiddleSolutionWithRecompute");
      
      return bestSuccessorRelation;
    }
    catch(Exception e)
    {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * code to read in a successor to bypass running search if best path is already known
   * @return
   */
  /*
  private SuccessorPath readInSuccessor()
  {
    try
    {
      //use buffering
      InputStream file = new FileInputStream( "successorFile" );
      InputStream buffer = new BufferedInputStream( file );
      ObjectInput input = new ObjectInputStream ( buffer );
      //deserialize the List
      SuccessorPath recoveredsuccessor = (SuccessorPath)input.readObject();
      input.close();
      return recoveredsuccessor;
    }
    catch(Exception e)
    {
      System.out.println("read in successor failed");
      return null;
    }
  }
*/
  private void writeSuccessorPathToFile(SuccessorPath bestSuccessorRelation)
  {
    try
    {
      OutputStream file = new FileOutputStream( successorFolder.toString() + sep + "successorFile" );
      OutputStream buffer = new BufferedOutputStream( file );
      ObjectOutput output = new ObjectOutputStream( buffer );
      output.writeObject(bestSuccessorRelation);
      output.close();
    }
    catch(Exception e)
    {
      System.out.println("cannot write successorpath to file");
    }
  }
}
