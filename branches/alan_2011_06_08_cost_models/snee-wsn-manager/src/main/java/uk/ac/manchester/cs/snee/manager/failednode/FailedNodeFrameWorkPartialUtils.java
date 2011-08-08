package uk.ac.manchester.cs.snee.manager.failednode;

import java.io.File;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOTUtils;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.RTUtils;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;

/**
 *@author stokesa6
 * class used to add utility functions for failed node framework
 **/
public class FailedNodeFrameWorkPartialUtils 
{

  private FailedNodeFrameWorkPartial ad;
  private String sep = System.getProperty("file.separator");
  
  public FailedNodeFrameWorkPartialUtils(FailedNodeFrameWorkPartial ad)
  {
    this.ad = ad;
  }
  
  /**
   * outputs a agenda in latex form into the autonomic manager.
   * @param agendaIOT
   * @param newIOT
   */
  public void outputNewAgendaImage(File outputFolder, IOT iot, String fileName)
  {
    try
    {
      ad.getAgenda().setID("newAgenda");
      AgendaIOTUtils output = new AgendaIOTUtils(ad.getAgenda(), iot, true);
      File agendaFolder = new File(outputFolder.toString() + sep + "Agendas");
      agendaFolder.mkdir();
      output.generateImage(agendaFolder.toString());
      output.exportAsLatex(agendaFolder.toString() + sep, fileName);
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

  public void outputAgendas(AgendaIOT newAgenda, AgendaIOT agenda, IOT oldIOT, IOT newIOT, File outputFolder) throws SNEEConfigurationException
  {
    AgendaIOTUtils oldOutput = new AgendaIOTUtils(agenda, oldIOT, false);
    AgendaIOTUtils output = new AgendaIOTUtils(newAgenda, newIOT, false);
    AgendaIOTUtils oldOutputMS = new AgendaIOTUtils(agenda, oldIOT, true);
    AgendaIOTUtils outputMS = new AgendaIOTUtils(newAgenda, newIOT, true);
    File agendaFolder = new File(outputFolder.toString() + sep + "Agendas");
    agendaFolder.mkdir();
    //bms
    output.generateImage(agendaFolder.toString());
    output.exportAsLatex(agendaFolder.toString() + sep + "newAgendaLatexBMS.tex");
    oldOutput.generateImage(agendaFolder.toString());
    oldOutput.exportAsLatex(agendaFolder.toString() + sep + "oldAgendaLatexBMS.tex");
    //ms
    outputMS.generateImage(agendaFolder.toString());
    outputMS.exportAsLatex(agendaFolder.toString() + sep + "newAgendaLatexMS.tex");
    oldOutputMS.generateImage(agendaFolder.toString());
    oldOutputMS.exportAsLatex(agendaFolder.toString() + sep + "oldAgendaLatexMS.tex");
    
  }
  
}
