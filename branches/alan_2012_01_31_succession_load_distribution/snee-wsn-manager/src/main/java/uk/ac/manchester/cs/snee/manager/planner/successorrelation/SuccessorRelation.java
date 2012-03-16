package uk.ac.manchester.cs.snee.manager.planner.successorrelation;

import java.io.File;
import java.util.HashMap;

import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.AutonomicManagerComponent;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.planner.PlannerUtils;
import uk.ac.manchester.cs.snee.manager.planner.common.SuccessorPath;
import uk.ac.manchester.cs.snee.manager.planner.successorrelation.tabu.TabuSearch;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;

public class SuccessorRelation extends AutonomicManagerComponent
{
  private static final long serialVersionUID = 891621010871659913L;
  private File successorFolder = null;
  private HashMap<String, RunTimeSite> runningSites;
  private MetadataManager _metadataManager;
  
  public SuccessorRelation(File plannerFolder, HashMap<String, RunTimeSite> runningSites,
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
      new PlannerUtils(successorFolder, this.manager).writeSuccessorToFile(bestSuccessorRelation.getSuccessorList());
      return bestSuccessorRelation;
    }
    catch(Exception e)
    {
      e.printStackTrace();
      return null;
    }
  }
}
