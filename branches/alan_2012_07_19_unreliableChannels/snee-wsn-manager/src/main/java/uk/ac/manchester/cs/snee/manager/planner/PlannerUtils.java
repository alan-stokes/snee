package uk.ac.manchester.cs.snee.manager.planner;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;

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
  
  /**
   * ouputs the energies left by the network once the qep has failed
   * @param successor
   */
  public void networkEnergyReport(HashMap<String, RunTimeSite> runtimeSites, File outputFolder)
  {
    try 
    {
      final PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outputFolder.toString() + sep + "energyReport")));
      Iterator<String> keys = runtimeSites.keySet().iterator();
      while(keys.hasNext())
      {
        String key = keys.next();
        RunTimeSite site = runtimeSites.get(key);
        Double leftOverEnergy = site.getCurrentEnergy();
        out.println("Node " + key + " has residual energy " + 
                    leftOverEnergy + " and qep Cost of " + site.getQepExecutionCost()) ; 
      }
      out.flush();
      out.close();
    }
    catch(Exception e)
    {
      System.out.println("couldnt write the energy report");
    }
  }
}
