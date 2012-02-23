package uk.ac.manchester.cs.snee.manager.planner;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAFUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.RTUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.planner.successorrelation.Successor;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;

public class SuccessorUtils
{
  private Successor successor;
  private String sep = System.getProperty("file.separator");
  
  
  public SuccessorUtils (Successor successor)
  {
    this.successor = successor;
  }
  
  public void exportSuccessor(String filename) 
  throws SNEEConfigurationException, SchemaMetadataException
  {
    SensorNetworkQueryPlan qep = successor.getQep();
    new DAFUtils(qep.getDAF()).exportAsDOTFile(filename + sep + "daf");
    new RTUtils(qep.getRT()).exportAsDotFile(filename + sep + "RT");
    new AgendaUtils(qep.getAgenda(), false).generateImage(filename + sep + "agenda");
  }
  
}
