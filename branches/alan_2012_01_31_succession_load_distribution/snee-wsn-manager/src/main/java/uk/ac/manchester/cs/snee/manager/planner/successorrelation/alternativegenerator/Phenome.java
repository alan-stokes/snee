package uk.ac.manchester.cs.snee.manager.planner.successorrelation.alternativegenerator;

import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.manager.planner.common.Successor;

public class Phenome implements Comparable<Phenome>
{
  
  //phenotype
  private double eliteFitness;
  private RT rt;
  private Successor successor;
  private double fitness;
  private Genome DNA;
  
  public Phenome (RT routingTree, Successor successor, Genome DNA)
  {
    this.rt = routingTree;
    this.DNA = DNA;
    this.setSuccessor(successor);
  }
  
  public Phenome (double fitness)
  {
    this.setFitness(fitness);
  }
  
  public Phenome (Genome DNA, int previousTimePeriod)
  {
    this.DNA = DNA;
    successor = new Successor(DNA.getTimePeriod(), previousTimePeriod);
  }
  
  @Override
  public int compareTo(Phenome o)
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
    return this.successor.calculateLifetime();
  }

  public void setEliteFitness(double eliteFitness)
  {
    this.eliteFitness = eliteFitness;
  }

  public double getEliteFitness()
  {
    return eliteFitness;
  }

  public void setSuccessor(Successor successor)
  {
    this.successor = successor;
  }

  public Successor getSuccessor()
  {
    return successor;
  }

  public void setFitness(double fitness)
  {
    this.fitness = fitness;
  }

  public double getFitness()
  {
    return fitness;
  }
  
  public int getGenomeTime()
  {
    return DNA.getTimePeriod();
  }

}
