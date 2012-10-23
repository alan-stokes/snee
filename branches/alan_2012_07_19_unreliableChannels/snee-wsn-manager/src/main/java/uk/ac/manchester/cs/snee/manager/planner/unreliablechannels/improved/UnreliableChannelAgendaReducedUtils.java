package uk.ac.manchester.cs.snee.manager.planner.unreliablechannels.improved;

import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import javax.imageio.ImageIO;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.CommunicationTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.FragmentTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.SleepTask;
import uk.ac.manchester.cs.snee.compiler.queryplan.Task;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class UnreliableChannelAgendaReducedUtils 
{

  /**
   * Logger for this class.
   */
  private static final Logger logger = Logger.getLogger(UnreliableChannelAgendaReducedUtils.class.getName());


  private static final int CELL_WIDTH = 100;

  private static final int CELL_HEIGHT = 20;

  IOT iot;

  UnreliableChannelAgendaReduced agenda;

  private boolean useMilliSeconds;
  
  private ArrayList<String> failedNodes;

  public UnreliableChannelAgendaReducedUtils(final UnreliableChannelAgendaReduced agenda, IOT iot, 
                                      final boolean useMilliSeconds,
                                      ArrayList<String> failedNodes) 
  {
    this.agenda = agenda;
    this.iot = iot;
    this.useMilliSeconds = useMilliSeconds;
    this.failedNodes = failedNodes;
  }

  // computes the width of the schedule image
  private int computeWidth() 
  {

    Iterator<Site> sitesIterator = this.agenda.siteIterator();
    int sitesCount = 0;
    while (sitesIterator.hasNext()) 
    {
      sitesIterator.next();
      sitesCount++;
    }
    sitesCount -= this.failedNodes.size();
    return (sitesCount + 2) * CELL_WIDTH;
  }

  // computes the height of the schedule image
  private int computeHeight() 
  {
    Iterator<Long> timeIterator = this.agenda.getStartTimes().iterator();
    int counter = 0;
    while(timeIterator.hasNext())
    {
      Long time = timeIterator.next();
      HashMap<Site, ArrayList<Task>> tasks = this.agenda.getTasks();
      Iterator<Site> siteIterator = tasks.keySet().iterator();
      int max =1;
      while(siteIterator.hasNext())
      {
        Site site = siteIterator.next();
        int siteCount = 0;
        Iterator<Task> tasksOfSite = tasks.get(site).iterator();
        while(tasksOfSite.hasNext())
        {
          Task t = tasksOfSite.next();
          if(t.getStartTime() == time)
            siteCount++;
        }
        if(siteCount > max)
          max = siteCount;
      }
      counter += max;
    }
    return (counter + 3) * CELL_HEIGHT;
  }

  public void generateImage() 
  throws SNEEConfigurationException 
  {

    String sep = System.getProperty("file.separator");
    String outputDir = SNEEProperties.getSetting(
        SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR) +
        sep + agenda.getQueryName() + sep + "query-plan";
    generateImage(outputDir);
  }


  public void generateImage(String outputDir) 
  throws SNEEConfigurationException 
  {
    generateImage(outputDir, this.agenda.getID());
  }    

  /**
   * deals with a sites agenda
   * @param site
   * @param xpos
   * @param ypos
   * @param g2
   * @param startTimeIter
   * @param spaces 
   */
  private int outputSiteAgenda(Site site, Integer xpos, int ypos, 
                                Graphics2D g2, Iterator<Long> startTimeIter, HashMap<Long, Integer> spaces)
  {
    if (this.agenda.hasTasks(site)) 
    {
      ypos = 20;
      g2.setColor(Color.BLACK);
      g2.setFont(new Font("Arial", Font.BOLD, 12));
      g2.drawString("Node " + site.getID(), xpos + 15, ypos - 5);
      g2.setFont(new Font("Arial", Font.PLAIN, 12));
      HashMap<Integer, Integer> timesUsedPosition = new HashMap<Integer, Integer>();
      
      final Iterator<Task> taskIter = this.agenda.taskIterator(site);
      while (taskIter.hasNext()) 
      {
        final Task task = taskIter.next();
        ypos = 20;
        startTimeIter = this.agenda.startTimeIterator();
        Long sTime = new Long(-1);
        //try to locate new time stamp. (possible to have more than 1 task at the same time slot
        // given the agenda now having redundant tranmssions and receiveings. 
        //get to near neighbourhood
        while (sTime.intValue() != task.getStartTime())
        {
          sTime = startTimeIter.next();
          if(spaces.get(sTime) == 1)
            ypos += CELL_HEIGHT;
          if(spaces.get(sTime) > 1 && sTime.intValue() != task.getStartTime())
            ypos += (spaces.get(sTime) * CELL_HEIGHT);
        } 
        
        //get more precise for several tasks.
        if(timesUsedPosition.get(ypos) == null)
          timesUsedPosition.put(ypos, 1);
        else
        {
          while(timesUsedPosition.get(ypos) != null)
          {
            ypos += CELL_HEIGHT;
          }
          timesUsedPosition.put(ypos, 1);
        }
          
       
        //set up task box
        if (task instanceof CommunicationTask) 
        {
          if(task.isRan())
          {
            g2.setColor(Color.YELLOW);
            g2.fill(new Rectangle(xpos, ypos - CELL_HEIGHT, CELL_WIDTH, CELL_HEIGHT));
            g2.setColor(Color.BLUE);
            g2.draw(new Rectangle(xpos, ypos - CELL_HEIGHT, CELL_WIDTH, CELL_HEIGHT));
            String output = task.toString();
            g2.drawString(output, xpos + 12, ypos + 12 - CELL_HEIGHT);
          }
        }
        else 
        {
          g2.setColor(Color.WHITE);
          g2.fill(new Rectangle(xpos, ypos - CELL_HEIGHT, CELL_WIDTH, CELL_HEIGHT));
          g2.setColor(Color.BLUE);
          g2.draw(new Rectangle(xpos, ypos - CELL_HEIGHT, CELL_WIDTH, CELL_HEIGHT));
          String output = task.toString();
          g2.drawString(output, xpos + 12, ypos + 12 - CELL_HEIGHT);
        }

      }
      xpos += CELL_WIDTH;
    }
    return xpos;
  }

  public final void exportAsLatex() throws SNEEConfigurationException
  {
    String sep = System.getProperty("file.separator");
    String outputDir = SNEEProperties.getSetting(
        SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR) +
        sep + agenda.getQueryName() + sep + "query-plan" + sep + "query-planLatex";
    exportAsLatex(outputDir);
  }

  public final void exportAsLatex(final String outputFileName) 
  {
    try 
    {
      final PrintWriter out = new PrintWriter(new BufferedWriter(
          new FileWriter(outputFileName)));

      out.println("%This latex has been generated by the SNEE optimizer");
      out.println("\\scriptsize");

      int siteCount = 0;
      final StringBuffer siteStringBuff = new StringBuffer();


      Iterator<Site> siteIter = this.iot.siteIterator(TraversalOrder.POST_ORDER);
      while (siteIter.hasNext()) 
      {
        final Site site = siteIter.next();
        siteStringBuff.append("& " + site.getID());
        siteCount++;
      }

      out.println("\\begin{tabular}{|l|" + Utils.pad("c", siteCount)
          + "|}");
      //out.println("\\begin{tabular*}{6.4cm}{|p{0.8cm}|"+Utils.pad("p{0.4cm}",siteCount)+"|}");
      //out.println("\\begin{tabular}{|p{0.5cm}|"+Utils.pad("p{0.14cm}",siteCount)+"|}");
      out.println("\\hline");

      //temporary for ICDE'08 paper
      //out.println("\\multirow{2}{*}{\\textbf{Time}} & \\multicolumn{"+siteCount+"}{|c|}{\\textbf{Sites}} \\\\");
      out.println("\\multirow{2}{*}{\\textsf{Time (ms)}} & \\multicolumn{"
          + siteCount + "}{|c|}{\\textsf{Sites}} \\\\");
      out.println(siteStringBuff.toString() + " \\\\");
      out.println("\\hline");

      final Iterator<Long> startTimeIter = agenda.startTimeIterator();
      while (startTimeIter.hasNext()) 
      {
        final long startTime = startTimeIter.next().intValue();
        out.print("\\texttt{" + Agenda.bmsToMs(startTime) + "} ");

        siteIter = this.iot.siteIterator(TraversalOrder.POST_ORDER);
        while (siteIter.hasNext()) 
        {
          final Site site = siteIter.next();

          final Task t = agenda.getTask(startTime, site);
          if (t == null) 
          {
            out.print("& ");
          } 
          else if (t instanceof SleepTask) 
          {
            out.print("& \\multicolumn{" + siteCount
                + "}{|c|}{sleeping}");
            break;
          } 
          else if (t instanceof CommunicationTask) 
          {
            final CommunicationTask ct = (CommunicationTask) t;
            if (ct.getMode() == CommunicationTask.RECEIVE) 
            {
              out.print("& \\textit{rx" + ct.getSourceID() + "} ");
            } 
            else if (ct.getMode() == CommunicationTask.TRANSMIT)
            {
              out.print("& \\textit{tx" + ct.getDestID() + "} ");
            }
            else if(ct.getMode() == CommunicationTask.ACKRECEIVE)
              out.print("& \\textit{rxa" + ct.getSourceID() + "} ");
            else
              out.print("& \\textit{txa" + ct.getDestID() + "} ");
          } 
          else if (t instanceof FragmentTask) 
          {
            final FragmentTask ft = (FragmentTask) t;
            if (ft.getFragment().isLeaf()) 
            {
              out.print("& \\texttt{F" + ft.getFragment().getID()
                  + "$_{" + ft.getOccurrence() + "}$} ");
            } 
            else 
            {
              out.print("& \\texttt{F" + ft.getFragment().getID()
                  + "} ");
            }
          }
        }
        out.println(" \\\\");
      }

      out.println("\\hline");
      out.println("\\end{tabular}");
      out.println("\\normalsize");
      out.println("%End of generated Latex");
      out.close();
    } 
    catch (final IOException e) 
    {
      logger.warn("Unable to produce latex agenda", e);
    }
  }

  public final void exportAsLatex(final String outputDirName,
      final String outputFileName) 
  {
    this.exportAsLatex(outputDirName + outputFileName);
  }

  public void generateImage(String outputDir, String fileName)
  throws SNEEConfigurationException 
  {
    String sep = System.getProperty("file.separator");
    String pngFilePath = "";
    if(this.useMilliSeconds)  
      pngFilePath = outputDir + sep + fileName + "MS.png";
    else
      pngFilePath = outputDir + sep + fileName + "BMS.png";

    final BufferedImage offImage = new BufferedImage(this.computeWidth(),
        this.computeHeight(), BufferedImage.TYPE_INT_RGB);

    final Graphics2D g2 = offImage.createGraphics();

    g2.setColor(Color.WHITE);
    g2.fill(new Rectangle(0, 0, this.computeWidth(), this.computeHeight()));

    Integer xpos = 20;
    Integer ypos = 20;

    g2.setColor(Color.BLACK);
    HashMap<Long, Integer> spaces = new HashMap<Long, Integer>();
    
    Iterator<Long> startTimeIter = this.agenda.startTimeIterator();
    int previousMax = 1;
    while (startTimeIter.hasNext())
    {
      final Long startTime = startTimeIter.next();
      HashMap<Site, ArrayList<Task>> tasks = this.agenda.getTasks();
      Iterator<Site> siteIterator = tasks.keySet().iterator();
      int max =1;
      while(siteIterator.hasNext())
      {
        Site site = siteIterator.next();
        int siteCount = 0;
        Iterator<Task> tasksOfSite = tasks.get(site).iterator();
        while(tasksOfSite.hasNext())
        {
          Task t = tasksOfSite.next();
          if(t.getStartTime() == startTime)
            siteCount++;
        }
        if(siteCount > max)
          max = siteCount;
      }
      spaces.put(startTime, max);
      ypos += (CELL_HEIGHT * (previousMax - 1));
      previousMax = max;
      g2.setFont(new Font("Arial", Font.BOLD, 12));

      if (this.useMilliSeconds) 
      {
        g2.drawString(new Long(Agenda.bmsToMs(startTime)).toString(), xpos, ypos + 12);
      } 
      else 
      {
        g2.drawString(startTime.toString(), xpos, ypos + 12); 
      }
      g2.setFont(new Font("Arial", Font.PLAIN, 12));
      ypos += CELL_HEIGHT;
    }

    xpos = xpos + CELL_WIDTH ;

    final Iterator<Site> siteIter = this.iot.siteIterator(TraversalOrder.POST_ORDER);
    while (siteIter.hasNext()) 
    {
      Site site = siteIter.next();
      if(site.getID().equals(this.iot.getRoot().getSite().getID()))
      {
        site = (Site) this.agenda.getSiteByID(site);
        xpos = outputSiteAgenda(site, xpos, ypos, g2, startTimeIter, spaces);
      }
      ArrayList<String> clusterSites = agenda.getActiveLogicalOverlay().getActiveEquivilentNodes(site.getID());
      Iterator<String> clusterSitesIterator = clusterSites.iterator();
      while(clusterSitesIterator.hasNext())
      {
        String clusterNodeID = clusterSitesIterator.next();
        if(!this.failedNodes.contains(clusterNodeID))
        {
          site = (Site) this.agenda.getSiteByID(clusterNodeID);
          xpos = outputSiteAgenda(site, xpos, ypos, g2, startTimeIter, spaces);
        }
      }
    }
    g2.drawString(agenda.getDescendantsString(), 25, ypos + CELL_HEIGHT);

    try 
    {
      boolean status;

      final File outputfile = new File(pngFilePath);
      status = ImageIO.write(offImage, "png", outputfile);

      if (status == false) 
        logger.warn("No png writer found for schedule image type");

    } 
    catch (final IOException e) 
    {
      logger.warn("Error encountered writing agenda image.");
      System.out.println("Error encountered writing agenda image.");
      System.exit(0);
    }
    
  }

}
