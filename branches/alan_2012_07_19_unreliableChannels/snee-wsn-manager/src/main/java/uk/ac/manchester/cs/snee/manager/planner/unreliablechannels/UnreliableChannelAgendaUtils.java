package uk.ac.manchester.cs.snee.manager.planner.unreliablechannels;

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

public class UnreliableChannelAgendaUtils 
{

  /**
   * Logger for this class.
   */
  private static final Logger logger = Logger.getLogger(UnreliableChannelAgendaUtils.class.getName());


  private static final int CELL_WIDTH = 100;

  private static final int CELL_HEIGHT = 20;

  IOT iot;

  UnreliableChannelAgenda agenda;

  private boolean useMilliSeconds;

  public UnreliableChannelAgendaUtils(final UnreliableChannelAgenda agenda, IOT iot, 
                                      final boolean useMilliSeconds) 
  {
    this.agenda = agenda;
    this.iot = iot;
    this.useMilliSeconds = useMilliSeconds;
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
    
    return (sitesCount + 2) * CELL_WIDTH;
  }

  // computes the height of the schedule image
  private int computeHeight() 
  {
    return (this.agenda.getStartTimes().size() + 3) * CELL_HEIGHT;
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

    String sep = System.getProperty("file.separator");
    String pngFilePath = "";
    if(this.useMilliSeconds)  
      pngFilePath = outputDir + sep + agenda.getID() + "MS.png";
    else
      pngFilePath = outputDir + sep + agenda.getID() + "BMS.png";

    final BufferedImage offImage = new BufferedImage(this.computeWidth(),
        this.computeHeight(), BufferedImage.TYPE_INT_RGB);
    final Graphics2D g2 = offImage.createGraphics();

    g2.setColor(Color.WHITE);
    g2.fill(new Rectangle(0, 0, this.computeWidth(), this.computeHeight()));

    Integer xpos = 20;
    Integer ypos = 20;

    g2.setColor(Color.BLACK);
    Iterator<Long> startTimeIter = this.agenda.startTimeIterator();
    while (startTimeIter.hasNext())
    {
      final Long startTime = startTimeIter.next();
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

    xpos = 80;

    final Iterator<Site> siteIter = this.iot.siteIterator(TraversalOrder.POST_ORDER);
    while (siteIter.hasNext()) 
    {
      Site site = siteIter.next();
      xpos = outputSiteAgenda(site, xpos, ypos, g2, startTimeIter);
      ArrayList<String> clusterSites = agenda.getLogicalOverlayNetwork().getEquivilentNodes(site.getID());
      Iterator<String> clusterSitesIterator = clusterSites.iterator();
      while(clusterSitesIterator.hasNext())
      {
        String clusterNodeID = clusterSitesIterator.next();
        site = (Site) this.iot.getSiteFromID(clusterNodeID);
        xpos = outputSiteAgenda(site, xpos, ypos, g2, startTimeIter);
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
    }

  }    

  /**
   * deals with a sites agenda
   * @param site
   * @param xpos
   * @param ypos
   * @param g2
   * @param startTimeIter
   */
  private int outputSiteAgenda(Site site, Integer xpos, int ypos, 
                                Graphics2D g2, Iterator<Long> startTimeIter)
  {
    ypos = 20;
    g2.setColor(Color.BLACK);
    g2.setFont(new Font("Arial", Font.BOLD, 12));
    g2.drawString("Node " + site.getID(), xpos + 15, ypos - 5);

    startTimeIter = this.agenda.startTimeIterator();

    g2.setFont(new Font("Arial", Font.PLAIN, 12));

    if (this.agenda.hasTasks(site)) 
    {
      final Iterator<Task> taskIter = this.agenda.taskIterator(site);
      while (taskIter.hasNext()) 
      {
        final Task task = taskIter.next();

        Long sTime;
        do 
        {
          sTime = startTimeIter.next();
          ypos += CELL_HEIGHT;
        } while (sTime.intValue() != task.getStartTime());

        if (task instanceof CommunicationTask) 
        {
          g2.setColor(Color.YELLOW);
        }
        else 
          g2.setColor(Color.WHITE);
        g2.fill(new Rectangle(xpos, ypos - CELL_HEIGHT, CELL_WIDTH, CELL_HEIGHT));
        g2.setColor(Color.BLUE);
        g2.draw(new Rectangle(xpos, ypos - CELL_HEIGHT, CELL_WIDTH, CELL_HEIGHT));
        String output = task.toString();
        g2.drawString(output, xpos + 12, ypos + 12 - CELL_HEIGHT);

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
              out
              .print("& \\textit{rx" + ct.getSourceID()
                  + "} ");
            } 
            else 
            {
              out.print("& \\textit{tx" + ct.getDestID() + "} ");
            }
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

}
