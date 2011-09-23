package uk.ac.manchester.cs.snee.manager.planner;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.costmodels.cardinalitymodel.CardinalityEstimatedCostModel;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;

public class Plotter implements Serializable
{

  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -2846057863098603796L;
  
  private File outputFolder = null;
  private int queryID = 1;
  private String sep = System.getProperty("file.separator");
  private File energyPlotFile = null;
  private double energyYMax = 0;
  private File timePlotFile = null;
  private double timeYMax = 0;
  private File qepCostPlotFile = null;
  private double qepYMax = 0;
  private File lifetimePlotFile = null;
  private double lifetimeYMax = 0;
  private File cyclesBurnedPlotFile = null;
  private double cyclesBurnedYMax = 0;
  private File cyclesMissedPlotFile = null;
  private double cyclesMissedYMax = 0;
  private File cyclesLeftPlotFile = null;
  private double cyclesLeftYMax = 0;
  private File tuplesLeftPlotFile = null;
  private double tuplesLeftYMax = 0;
  private File tuplesMissedPlotFile = null;
  private double tuplesMissedYMax = 0;
  private File tuplesBurnedPlotFile = null;
  private double tuplesBurnedYMax = 0;
  private int numberOfAdaptations = 0;
  private BufferedWriter energyWriter;
  private BufferedWriter timeWriter;
  private BufferedWriter qepWriter;
  private BufferedWriter lifetimeWriter;
  private BufferedWriter cyclesBurnedWriter;
  private BufferedWriter cyclesMissedWriter;
  private BufferedWriter cyclesLeftWriter;
  private BufferedWriter tuplesLeftWriter;
  private BufferedWriter tuplesMissedWriter;
  private BufferedWriter tuplesBurnedWriter;
  
  public Plotter (File outputFolder, int numberOfAdaptations, AutonomicManagerImpl manager) 
  throws IOException
  {
    this.outputFolder = outputFolder;
    outputFolder = new File("plots");
    if(outputFolder.exists())
    {
      manager.deleteFileContents(outputFolder);
      outputFolder.mkdir();
    }
    else
      outputFolder.mkdir();
    
    energyPlotFile = new File(outputFolder.toString() + sep + "energyPlot");
    timePlotFile = new File(outputFolder.toString() + sep + "timePlot");
    qepCostPlotFile = new File(outputFolder.toString() + sep + "qepPlot");
    lifetimePlotFile = new File(outputFolder.toString() + sep + "lifetimePlot");
    cyclesBurnedPlotFile = new File(outputFolder.toString() + sep + "cyclesBurnedPlot");
    cyclesMissedPlotFile = new File(outputFolder.toString() + sep + "cyclesMissedPlot");
    cyclesLeftPlotFile = new File(outputFolder.toString() + sep + "cyclesLeftPlot");
    tuplesLeftPlotFile = new File(outputFolder.toString() + sep + "tuplesLeftPlot");
    tuplesMissedPlotFile = new File(outputFolder.toString() + sep + "tuplesMissedPlot");
    tuplesBurnedPlotFile = new File(outputFolder.toString() + sep + "tuplesBurnedPlot");
    this.numberOfAdaptations = numberOfAdaptations;
    if(energyPlotFile.exists())
    {
      energyPlotFile.delete();
    }
    if(timePlotFile.exists())
    {
      timePlotFile.delete();
    }
    if(qepCostPlotFile.exists())
    {
      qepCostPlotFile.delete();
    }
    if(lifetimePlotFile.exists())
    {
      lifetimePlotFile.delete();
    }
    if(cyclesBurnedPlotFile.exists())
    {
      cyclesBurnedPlotFile.delete();
    }
    if(cyclesMissedPlotFile.exists())
    {
      cyclesMissedPlotFile.delete();
    }
    if(cyclesLeftPlotFile.exists())
    {
      cyclesLeftPlotFile.delete();
    }
    if(tuplesLeftPlotFile.exists())
    {
      tuplesLeftPlotFile.delete();
    }
    if(tuplesMissedPlotFile.exists())
    {
      tuplesMissedPlotFile.delete();
    }
    if(tuplesBurnedPlotFile.exists())
    {
      tuplesBurnedPlotFile.delete();
    }
    
    energyWriter = new BufferedWriter(new FileWriter(energyPlotFile, true));
    timeWriter = new BufferedWriter(new FileWriter(timePlotFile, true));
    qepWriter = new BufferedWriter(new FileWriter(qepCostPlotFile, true));
    lifetimeWriter = new BufferedWriter(new FileWriter(lifetimePlotFile));
    cyclesBurnedWriter =  new BufferedWriter(new FileWriter(cyclesBurnedPlotFile, true));
    cyclesMissedWriter = new BufferedWriter(new FileWriter(cyclesMissedPlotFile, true));
    cyclesLeftWriter = new BufferedWriter(new FileWriter(cyclesLeftPlotFile, true));
    tuplesLeftWriter = new BufferedWriter(new FileWriter(tuplesLeftPlotFile, true));
    tuplesMissedWriter = new BufferedWriter(new FileWriter(tuplesMissedPlotFile, true));
    tuplesBurnedWriter = new BufferedWriter(new FileWriter(tuplesBurnedPlotFile, true));
  }
  
  public void plot(Adaptation global, Adaptation partial, Adaptation local) 
  throws 
  IOException, OptimizationException
  {
    DecimalFormat df = new DecimalFormat("#.#####");
    
    energyWriter.write(queryID + " ");
    if(global != null)
    {
      energyWriter.write(df.format(global.getEnergyCost()) + " ");
      energyYMax = Math.max(energyYMax, global.getEnergyCost());
    }
    if(partial != null)
    {
      energyWriter.write(df.format(partial.getEnergyCost()) + " ");
      energyYMax = Math.max(energyYMax, partial.getEnergyCost());
    }
    if(local != null)
    {
      energyWriter.write(df.format(local.getEnergyCost()) + " ");
      energyYMax = Math.max(energyYMax, local.getEnergyCost());
    }
    energyWriter.write("\n");
    
    timeWriter.write(queryID + " ");
    if(global != null)
    {
      timeWriter.write(df.format(global.getTimeCost()) + " ");
      timeYMax = Math.max(timeYMax, global.getTimeCost());
    }
    if(partial != null)
    {
      timeWriter.write(df.format(partial.getTimeCost()) + " ");
      timeYMax = Math.max(timeYMax, partial.getTimeCost());
    }
    if(local != null)
    {
      timeWriter.write(df.format(local.getTimeCost()) + " ");
      timeYMax = Math.max(timeYMax, local.getTimeCost());
    }
    timeWriter.write("\n");
    
    qepWriter.write(queryID + " ");
    if(global != null)
    {
      qepWriter.write(df.format(global.getRuntimeCost()) + " ");
      qepYMax = Math.max(qepYMax, global.getRuntimeCost());
    }
    if(partial != null)
    {
      qepWriter.write(df.format(partial.getRuntimeCost()) + " ");
      qepYMax = Math.max(qepYMax, partial.getRuntimeCost());
    }
    if(local != null)
    {
      qepWriter.write(df.format(local.getRuntimeCost()) + " ");
      qepYMax = Math.max(qepYMax, local.getRuntimeCost());
    }
    qepWriter.write("\n");
    
    lifetimeWriter.write(queryID + " ");
    if(global != null)
    {
      lifetimeWriter.write(df.format(global.getLifetimeEstimate()) + " ");
      lifetimeYMax = Math.max(lifetimeYMax, global.getLifetimeEstimate());
    }
    if(partial != null)
    {
      lifetimeWriter.write(df.format(partial.getLifetimeEstimate()) + " ");
      lifetimeYMax = Math.max(lifetimeYMax, partial.getLifetimeEstimate());
    }
    if(local != null)
    {
      lifetimeWriter.write(df.format(local.getLifetimeEstimate()) + " ");
      lifetimeYMax = Math.max(lifetimeYMax, local.getLifetimeEstimate());
    }
    lifetimeWriter.write("\n");
    
    cyclesBurnedWriter.write(queryID + " ");
    if(global != null)
    {
      cyclesBurnedWriter.write(df.format(global.getEnergyCost() / global.getRuntimeCost()) + " ");
      cyclesBurnedYMax = Math.max(cyclesBurnedYMax, global.getEnergyCost() / global.getRuntimeCost());
    }
    if(partial != null)
    {
      cyclesBurnedWriter.write(df.format(partial.getEnergyCost() / partial.getRuntimeCost()) + " ");
      cyclesBurnedYMax = Math.max(cyclesBurnedYMax, partial.getEnergyCost() / partial.getRuntimeCost());
    }
    if(local != null)
    {
      cyclesBurnedWriter.write(df.format(local.getEnergyCost() / local.getRuntimeCost()) + " ");
      cyclesBurnedYMax = Math.max(cyclesBurnedYMax, local.getEnergyCost() / local.getRuntimeCost());
    }
    cyclesBurnedWriter.write("\n");   
    
    cyclesMissedWriter.write(queryID + " ");
    if(global != null)
    {
      cyclesMissedWriter.write(df.format(global.getTimeCost() / global.getNewQep().getAgendaIOT().getLength_bms(false)) + " ");
      cyclesMissedYMax = Math.max(cyclesMissedYMax, global.getTimeCost() / global.getNewQep().getAgendaIOT().getLength_bms(false));
    }
    if(partial != null)
    {
      cyclesMissedWriter.write(df.format(partial.getTimeCost() / partial.getNewQep().getAgendaIOT().getLength_bms(false)) + " ");
      cyclesMissedYMax = Math.max(cyclesMissedYMax, partial.getTimeCost() / partial.getNewQep().getAgendaIOT().getLength_bms(false));
    }
    if(local != null)
    {
      cyclesMissedWriter.write(df.format(local.getTimeCost() / local.getNewQep().getAgendaIOT().getLength_bms(false)) + " ");
      cyclesMissedYMax = Math.max(cyclesMissedYMax, local.getTimeCost() / local.getNewQep().getAgendaIOT().getLength_bms(false));
    }
    cyclesMissedWriter.write("\n");   

    cyclesLeftWriter.write(queryID + " ");
    if(global != null)
    {
      cyclesLeftWriter.write(df.format(global.getLifetimeEstimate() / global.getNewQep().getAgendaIOT().getLength_bms(false)) + " ");
      cyclesLeftYMax = Math.max(cyclesLeftYMax, global.getLifetimeEstimate() / global.getNewQep().getAgendaIOT().getLength_bms(false));
    }
    if(partial != null)
    {
      cyclesLeftWriter.write(df.format(partial.getLifetimeEstimate() / partial.getNewQep().getAgendaIOT().getLength_bms(false)) + " ");
      cyclesLeftYMax = Math.max(cyclesLeftYMax, partial.getLifetimeEstimate() / partial.getNewQep().getAgendaIOT().getLength_bms(false));
    }
    if(local != null)
    {
      cyclesLeftWriter.write(df.format(local.getLifetimeEstimate() / local.getNewQep().getAgendaIOT().getLength_bms(false)) + " ");
      cyclesLeftYMax = Math.max(cyclesLeftYMax, local.getLifetimeEstimate() / local.getNewQep().getAgendaIOT().getLength_bms(false));
    }
    cyclesLeftWriter.write("\n"); 

    float globalTupleCount = 0;
    float partialTupleCount = 0;
    float localTupleCount = 0;
    if(global != null)
    {
      CardinalityEstimatedCostModel tupleModel = new CardinalityEstimatedCostModel(global.getNewQep());
      tupleModel.runModel();
      globalTupleCount = tupleModel.returnAgendaExecutionResult();
    }
    if(partial != null)
    {
      CardinalityEstimatedCostModel tupleModel = new CardinalityEstimatedCostModel(partial.getNewQep());
      tupleModel.runModel();
      partialTupleCount = tupleModel.returnAgendaExecutionResult();
    }
    if(local != null)
    {
      CardinalityEstimatedCostModel tupleModel = new CardinalityEstimatedCostModel(local.getNewQep());
      tupleModel.runModel();
      localTupleCount = tupleModel.returnAgendaExecutionResult();
    }
    
    tuplesLeftWriter.write(queryID + " ");
    if(global != null)
    {
      tuplesLeftWriter.write(df.format(globalTupleCount * (global.getLifetimeEstimate() / global.getNewQep().getAgendaIOT().getLength_bms(false))) + " ");
      tuplesLeftYMax = Math.max(tuplesLeftYMax, globalTupleCount * (global.getLifetimeEstimate() / global.getNewQep().getAgendaIOT().getLength_bms(false)));
    }
    if(partial != null)
    {
      tuplesLeftWriter.write(df.format(partialTupleCount * (partial.getLifetimeEstimate() / partial.getNewQep().getAgendaIOT().getLength_bms(false))) + " ");
      tuplesLeftYMax = Math.max(tuplesLeftYMax, partialTupleCount * (partial.getLifetimeEstimate() / partial.getNewQep().getAgendaIOT().getLength_bms(false)));
    }
    if(local != null)
    {
      tuplesLeftWriter.write(df.format(localTupleCount * (local.getLifetimeEstimate() / local.getNewQep().getAgendaIOT().getLength_bms(false))) + " ");
      tuplesLeftYMax = Math.max(tuplesLeftYMax, localTupleCount * (local.getLifetimeEstimate() / local.getNewQep().getAgendaIOT().getLength_bms(false)));
    }
    tuplesLeftWriter.write("\n"); 
    
    tuplesMissedWriter.write(queryID + " ");
    if(global != null)
    {
      tuplesMissedWriter.write(df.format((global.getTimeCost() / global.getNewQep().getAgendaIOT().getLength_bms(false)) * globalTupleCount) + " ");
      tuplesMissedYMax = Math.max(tuplesMissedYMax, (global.getTimeCost() / global.getNewQep().getAgendaIOT().getLength_bms(false)) * globalTupleCount);
    }
    if(partial != null)
    {
      tuplesMissedWriter.write(df.format((partial.getTimeCost() / partial.getNewQep().getAgendaIOT().getLength_bms(false)) * partialTupleCount) + " ");
      tuplesMissedYMax = Math.max(tuplesMissedYMax, (partial.getTimeCost() / partial.getNewQep().getAgendaIOT().getLength_bms(false)) * partialTupleCount);
    }
    if(local != null)
    {
      tuplesMissedWriter.write(df.format((local.getTimeCost() / local.getNewQep().getAgendaIOT().getLength_bms(false)) * localTupleCount) + " ");
      tuplesMissedYMax = Math.max(tuplesMissedYMax, (local.getTimeCost() / local.getNewQep().getAgendaIOT().getLength_bms(false)) * localTupleCount);
    }
    tuplesMissedWriter.write("\n"); 
    
    tuplesBurnedWriter.write(queryID + " ");
    if(global != null)
    {
      tuplesBurnedWriter.write(df.format((global.getEnergyCost() / global.getRuntimeCost()) * globalTupleCount) + " ");
      tuplesBurnedYMax =  Math.max(tuplesBurnedYMax, (global.getEnergyCost() / global.getRuntimeCost()) * globalTupleCount);
    }
    if(partial != null)
    {
      tuplesBurnedWriter.write(df.format((partial.getEnergyCost() / partial.getRuntimeCost()) * partialTupleCount) + " ");
      tuplesBurnedYMax =  Math.max(tuplesBurnedYMax, (partial.getEnergyCost() / partial.getRuntimeCost()) * partialTupleCount);
    }
    if(local != null)
    {
      tuplesBurnedWriter.write(df.format((local.getEnergyCost() / local.getRuntimeCost()) * localTupleCount) + " ");
      tuplesBurnedYMax =  Math.max(tuplesBurnedYMax, (local.getEnergyCost() / local.getRuntimeCost()) * localTupleCount);
    }
    tuplesBurnedWriter.write("\n"); 
    
    energyWriter.flush();
    timeWriter.flush(); 
    qepWriter.flush(); 
    lifetimeWriter.flush();
    cyclesBurnedWriter.flush();
    cyclesMissedWriter.flush(); 
    cyclesLeftWriter.flush();
    tuplesLeftWriter.flush(); 
    tuplesMissedWriter.flush();
    tuplesBurnedWriter.flush();
    
    try
    {
    if(numberOfAdaptations == 3)
    {
      Runtime rt = Runtime.getRuntime();
      File bash = new File("src/main/resources/bashScript/plotGnuplot3");
      String location = bash.getAbsolutePath();
      String command = location + " \"topologies\" \"adaptation cost (J)\" " + 
      new Integer(queryID+1).toString() + " " + new Double(this.energyYMax).toString() +
      " \"" + outputFolder.getAbsolutePath() + sep + "energy" + "\" \"" +
      energyPlotFile.getAbsolutePath() + "\"  \"global\" " + "\"partial\" \"local\"";
      rt.exec(command);
    }
    else
    {
      Runtime rt = Runtime.getRuntime();
      File bash = new File("src/main/resources/bashScript/plotGnuplot2");
      String location = bash.getAbsolutePath();
      String command = location + " \"topologies\" \"adaptation cost (J)\" " +
      new Integer(queryID+1).toString() + " " + new Double(this.energyYMax).toString() +
      " \"" + outputFolder.getAbsolutePath() + sep + "energy" + "\" \"" +
      energyPlotFile.getAbsolutePath() + "\"  \"global\" " + "\"partial\"";
      rt.exec(command);
    }
    }
    catch(Exception e)
    {
      System.out.println(e.getMessage());
      e.printStackTrace();
      System.exit(0);
    }
    
  }
  
}
