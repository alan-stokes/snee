package uk.ac.manchester.cs.snee.manager.planner.common;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.iot.IOTUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAFUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.RTUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;

public class OverlaySuccessorUtils
{
  private OverlaySuccessor successor;
  private String sep = System.getProperty("file.separator");
  
  
  public OverlaySuccessorUtils (OverlaySuccessor successor)
  {
    this.successor = successor;
  }
  
  public void exportSuccessor(String filename) 
  throws SNEEConfigurationException, SchemaMetadataException
  {
    SensorNetworkQueryPlan qep = successor.getQep();
    new IOTUtils(qep.getIOT(), qep.getCostParameters()).exportAsDotFileWithFrags(filename + sep + "iot", "", true);
    new DAFUtils(qep.getDAF()).exportAsDOTFile(filename + sep + "daf");
    new RTUtils(qep.getRT()).exportAsDotFile(filename + sep + "RT");
    new AgendaUtils(qep.getAgenda(), false).generateImage(filename + sep + "agenda");
  }
  
}
