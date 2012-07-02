package uk.ac.manchester.cs.snee.manager.planner.overlaysuccessorrelation.alternativegenerator;

import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.manager.failednode.cluster.LogicalOverlayNetwork;
import uk.ac.manchester.cs.snee.manager.planner.common.OverlaySuccessor;

public class OverlayPhenome implements Comparable<OverlayPhenome>
{
  
  //phenotype
  private double eliteFitness;
  private RT rt;
  private OverlaySuccessor OverlaySuccessor;
  private double fitness;
  private OverlayGenome DNA;
  private LogicalOverlayNetwork logicalNetwork;
  
  public OverlayPhenome (RT routingTree, OverlaySuccessor OverlaySuccessor, OverlayGenome DNA)
  {
    this.rt = routingTree;
    this.DNA = DNA;
    this.setOverlaySuccessor(OverlaySuccessor);
  }
  
  public OverlayPhenome (int fitness)
  {
    this.setFitness(fitness);
  }
  
  public OverlayPhenome (OverlayGenome DNA, int previousTimePeriod)
  {
    this.DNA = DNA;
    OverlaySuccessor = new OverlaySuccessor(previousTimePeriod);
  }
  
  @Override
  public int compareTo(OverlayPhenome o)
  {
    Double value = this.eliteFitness;
    Double otherValue = o.eliteFitness;
    return value.compareTo(otherValue);
  }

  public RT getRt()
  {
    return this.rt;
  }
  
  public void setRt(RT rt)
  {
    this.rt = rt;
  }
  
  public int elitefitness()
  {
    return this.OverlaySuccessor.calculateLifetime();
  }

  public void setEliteFitness(double eliteFitness)
  {
    this.eliteFitness = eliteFitness;
  }

  public double getEliteFitness()
  {
    return eliteFitness;
  }

  public void setOverlaySuccessor(OverlaySuccessor OverlaySuccessor)
  {
    this.OverlaySuccessor = OverlaySuccessor;
  }

  public OverlaySuccessor getOverlaySuccessor()
  {
    return OverlaySuccessor;
  }

  public void setFitness(int fitness)
  {
    this.fitness = fitness;
  }

  public double getFitness()
  {
    return fitness;
  }
  
  public String toString()
  {
    return DNA.toString();
  }

  public int getEstimatedLifetime() 
  {
	  return logicalNetwork.getEstimatedLifetime();
  }

  public void setOverlaySuccessor(LogicalOverlayNetwork logicalOverlay) 
  {
	this.logicalNetwork = logicalOverlay;
  }
}
