package uk.ac.manchester.cs.snee.autonomicmanager.anaylsiser;

import java.io.File;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOTUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.RTUtils;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;

public class AdapatationStrategyIntermediateUtils
{

  private AdapatationStrategyIntermediate ad;
  private String sep = System.getProperty("file.separator");
  
  public AdapatationStrategyIntermediateUtils(AdapatationStrategyIntermediate ad)
  {
    this.ad = ad;
  }
  
  /**
   * outputs a agenda in latex form into the autonomic manager.
   * @param agendaIOT
   * @param newIOT
   */
  public void outputNewAgendaImage(File outputFolder)
  {
    try
    {
      ad.getAgenda().setID("newAgenda");
      AgendaIOTUtils output = new AgendaIOTUtils(ad.getAgenda(), ad.getIOT(), true);
      File agendaFolder = new File(outputFolder.toString() + sep + "Agendas");
      agendaFolder.mkdir();
      output.generateImage(agendaFolder.toString());
      output.exportAsLatex(agendaFolder.toString() + sep, "newAgenda");
    }
    catch (SNEEConfigurationException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } 
    
  }
  
  /**
   * method used to output topology as a dot file.
   * @param string location and name of topology file.
   */
  public void outputTopologyAsDotFile(File outputFolder , String string)
  {
    try
    {
      
      File topFolder = new File(outputFolder.toString() + sep + "Topology");
      topFolder.mkdir();
      ad.getWsnTopology().exportAsDOTFile(topFolder.toString() + string);
    }
    catch (SchemaMetadataException e)
    {
      e.printStackTrace();
    }
  }
  
  /**
   * method used to output a route as a dot file inside an adaption
   */
  public void outputRouteAsDotFile(File outputFolder , String string, RT route)
  {
    try
    {
      
      File topFolder = new File(outputFolder.toString() + sep + "Route");
      topFolder.mkdir();
      new RTUtils(route).exportAsDOTFile(topFolder.toString() + sep + string);
    }
    catch (SchemaMetadataException e)
    {
      e.printStackTrace();
    }
  }
  
}
