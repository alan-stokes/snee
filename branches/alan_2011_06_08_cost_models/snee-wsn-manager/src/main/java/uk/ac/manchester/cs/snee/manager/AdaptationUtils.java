package uk.ac.manchester.cs.snee.manager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOTUtils;
import uk.ac.manchester.cs.snee.compiler.iot.IOTUtils;
import uk.ac.manchester.cs.snee.metadata.CostParameters;

public class AdaptationUtils
{
  private Adaptation adapt = null;
  private List<Adaptation> adaptList = null;
  private final String sep = System.getProperty("file.separator");
  private CostParameters costs;
  
  public AdaptationUtils(Adaptation adapt, CostParameters costs)
  {
    this.adapt = adapt;
    this.costs = costs;
  }
  
  public AdaptationUtils(List<Adaptation> adapt, CostParameters costs)
  {
    this.adaptList = adapt;
    this.costs = costs;
  }
  
  public void systemOutput()
  {
    if(adapt != null)
    {
      System.out.println(adapt.toString());
    }
    else
    {
      Iterator<Adaptation> adaptIterator = adaptList.iterator();
      while(adaptIterator.hasNext())
      {
        Adaptation cAdapt = adaptIterator.next();
        System.out.println(cAdapt.toString());
      }
    }
  }
  
  public void FileOutput(File outputFolder) 
  throws IOException
  {
    File adaptFolder = new File(outputFolder.toString() + sep + "Adaptations");
    adaptFolder.mkdir();
    File outputFile = new File(adaptFolder.toString() + sep + "adaptList");
    BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
    if(adapt != null)
    {
      writer.write(adapt.toString() + "\n");
    }
    else
    {
      Iterator<Adaptation> adaptIterator = adaptList.iterator();
      while(adaptIterator.hasNext())
      {
        Adaptation cAdapt = adaptIterator.next();
        writer.write(cAdapt.toString() + "\n");
      }
    }
    writer.flush();
    writer.close();
  }
  
  public void FileOutputFinalChoice(File outputFolder) 
  throws IOException, SNEEConfigurationException
  {
    File adaptFolder = new File(outputFolder.toString() + sep + "finalChoice");
    adaptFolder.mkdir();
    File outputFile = new File(adaptFolder.toString() + sep + "adaptList");
    BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
    writer.write(adapt.toString() + "\n");
    writer.flush();
    writer.close();
    new IOTUtils(adapt.getNewQep().getIOT(), costs).
                exportAsDotFileWithFrags(adaptFolder.toString() + sep + "NEWIOT", "", true);
    new IOTUtils(adapt.getOldQep().getIOT(), costs).
                exportAsDotFileWithFrags(adaptFolder.toString() + sep + "OLDIOT", "", true);
    new AgendaIOTUtils(adapt.getNewQep().getAgendaIOT(), adapt.getNewQep().getIOT(), false)
    .generateImage(adaptFolder.toString());
    new AgendaIOTUtils(adapt.getOldQep().getAgendaIOT(), adapt.getOldQep().getIOT(), false)
    .generateImage(adaptFolder.toString());
    new AgendaIOTUtils(adapt.getNewQep().getAgendaIOT(), adapt.getNewQep().getIOT(), false)
    .exportAsLatex(adaptFolder.toString() + sep, "final choice Agenda Latex");
    new AgendaIOTUtils(adapt.getOldQep().getAgendaIOT(), adapt.getOldQep().getIOT(), false)
    .exportAsLatex(adaptFolder.toString() + sep, "Old Agenda Latex");
    
  }
  
  
}
