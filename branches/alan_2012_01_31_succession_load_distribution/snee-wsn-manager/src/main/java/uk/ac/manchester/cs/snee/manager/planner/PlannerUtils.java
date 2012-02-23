package uk.ac.manchester.cs.snee.manager.planner;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.iot.IOTUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAFUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.RTUtils;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.planner.successorrelation.Successor;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;

public class PlannerUtils
{
   private List<Adaptation> adaptations;
   private Adaptation orginal;
   private File plannerOutputFolder = null;
   
   private String sep = System.getProperty("file.separator");
   private AutonomicManagerImpl manager;
    
   public PlannerUtils(List<Adaptation> adaptations, AutonomicManagerImpl manager, 
                       File plannerOutputFolder, Adaptation orginal)
   {
     this.adaptations = adaptations;
     this.manager = manager;
     this.plannerOutputFolder = plannerOutputFolder;
     this.orginal = orginal;
   }
   
   public PlannerUtils(Adaptation adaptation, AutonomicManagerImpl manager, 
       File plannerOutputFolder, Adaptation orginal)
   {
     
     this.adaptations = new ArrayList<Adaptation>();
     this.adaptations.add(adaptation);
     this.manager = manager;
     this.plannerOutputFolder = plannerOutputFolder;
     this.orginal = orginal;
   }
   
   public PlannerUtils(File plannerOutputFolder, AutonomicManagerImpl manager)
   {
     this.plannerOutputFolder = plannerOutputFolder;
     this.manager = manager;
   }
   
   public void writeSuccessorToFile(ArrayList<Successor> successorRelation) 
   throws SNEEConfigurationException, SchemaMetadataException
   {
     int successorCount = 1;
     File finalSolution = new File(plannerOutputFolder.toString() + sep + "finalSolution");
     if(finalSolution.exists())
     {
       manager.deleteFileContents(finalSolution);
       finalSolution.mkdir();
     }
     else
       finalSolution.mkdir();
     
     outputdotFile(successorRelation, finalSolution);
     Iterator<Successor> successorIterator = successorRelation.iterator();
     while(successorIterator.hasNext())
     {
       File successorFolder = new File(finalSolution.toString() + sep + "successor" + successorCount);
       if(successorFolder.exists())
       {
         manager.deleteFileContents(successorFolder);
         successorFolder.mkdir();
       }
       else
         successorFolder.mkdir();
       Successor successor =  successorIterator.next();
       new RTUtils(successor.getQep().getRT()).exportAsDotFile(successorFolder.toString() + sep + "rt");
       new DAFUtils(successor.getQep().getDAF()).exportAsDotFile(successorFolder.toString() + sep + "daf");
       new IOTUtils(successor.getQep().getIOT(), null).exportAsDOTFile(successorFolder.toString() + sep + "iot", "", true);
       new AgendaUtils(successor.getQep().getAgenda(), false).exportAsLatex(successorFolder.toString() + sep + "agenda");
       successorCount ++;
     }
   }

  private void outputdotFile(ArrayList<Successor> successorRelation, File objectFolder) 
  throws SNEEConfigurationException, SchemaMetadataException
  {
    try
    {
      
      
      int maxSize = successorRelation.size();
      BufferedWriter out = new BufferedWriter(new FileWriter(objectFolder.toString() + sep + "path.dot"));
      out.write("digraph \"successor path\" { \n size = \"8.5,11\"; \n rankdir=\"BT\"; \n label=\"successor Path\";");
      int counter = 0;
      while(counter <= maxSize)
      {
        out.write("\"" + counter + "\"; \n");
        counter++;
      }
      out.write("\n\n");
      counter = 0;
      int nextCounter = counter +1;
      Iterator<Successor> successorIterator = successorRelation.iterator();
      while(successorIterator.hasNext())
      {
        Successor successor = successorIterator.next();
        String file = mkdir(counter, objectFolder);
        new SuccessorUtils(successor).exportSuccessor(file);
        out.write("\"" + counter + "\" -> \"" + nextCounter + "\" [fontsize=9 label = \"" + successor.getAgendaCount() + "\"]; \n");
        counter++;
        nextCounter = counter +1;
      }
      out.write("\n\n }");
      out.flush();
      out.close();
      
      //write the lifetimes of each successor
      out = new BufferedWriter(new FileWriter(objectFolder.toString() + sep + "successorLifetimes.txt"));
      successorIterator = successorRelation.iterator();
      counter = 0;
      while(successorIterator.hasNext())
      {
        Successor successor = successorIterator.next();
        out.write(counter + " : " + successor.getLifetimeInAgendas() + " \n");
        counter++;
      }
      out.flush();
      out.close();
     
    }
    catch (IOException e)
    {
      System.out.println("successor dot relation could not be written");
      e.printStackTrace();
    }
  }

  private String mkdir(int counter, File objectFolder)
  {
    File file = new File(objectFolder.toString() + sep + "plan" + counter);
    if(file.exists())
    {
      manager.deleteFileContents(objectFolder);
      objectFolder.mkdir();
    }
    else
      file.mkdir();
    return file.toString();
  }

  public void writeObjectsToFile()
  {
    try
    {
      File objectFolder = new File(plannerOutputFolder.toString() + sep + "storedObjects");
      if(objectFolder.exists())
      {
        manager.deleteFileContents(objectFolder);
        objectFolder.mkdir();
      }
      else
        objectFolder.mkdir();
      
      Iterator<Adaptation> adIterator = adaptations.iterator();
      while(adIterator.hasNext())
      {
        Adaptation ad = adIterator.next();
          ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(objectFolder.toString() + sep + ad.getOverallID()));
          outputStream.writeObject(ad);
          outputStream.flush();
          outputStream.close();
      }
      
      ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(objectFolder.toString() + sep + orginal.getOverallID()));
      outputStream.writeObject(orginal);
      outputStream.flush();
      outputStream.close();
    }
    catch(Exception e)
    {
      System.out.println(e.getMessage());
      e.printStackTrace();
      System.exit(0);
    } 
  }
}
