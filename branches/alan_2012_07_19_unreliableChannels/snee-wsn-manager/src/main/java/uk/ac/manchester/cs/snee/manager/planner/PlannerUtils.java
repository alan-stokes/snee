package uk.ac.manchester.cs.snee.manager.planner;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
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
