package uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork;

import java.io.File;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOTUtils;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;

public class LogicalOverlayStrategyUtils
{
  private String sep = System.getProperty("file.separator");
  
  public LogicalOverlayStrategyUtils()
  {
  }
  
  public void outputAgendas(AgendaIOT newAgenda, AgendaIOT agenda, 
                            IOT oldIOT, IOT newIOT, File outputFolder) 
  throws 
  SNEEConfigurationException
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
