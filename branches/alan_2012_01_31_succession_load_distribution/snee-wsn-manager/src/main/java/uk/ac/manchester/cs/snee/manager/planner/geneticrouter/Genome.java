package uk.ac.manchester.cs.snee.manager.planner.geneticrouter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import uk.ac.manchester.cs.snee.compiler.queryplan.RT;

public class Genome implements Comparable<Genome>
{
   private ArrayList<Boolean> DNA;
   private final static int crossoverPercentage = 90;
   private final static int mutationPercentage = 2;
   private double fitness;
   private double eliteFitness;
   private RT rt;
   
  
  
   public Genome(int sink, int [] sources, int NumberOfSites)
   {
     DNA = new ArrayList<Boolean>(NumberOfSites);
     fillDNA(NumberOfSites);
     DNA.set(sink, true);
     for(int index = 0; index < sources.length; index++)
     {
       DNA.set(sources[index], true);
     }
   }
   
   private void fillDNA(int numberOfSites)
  {
    for(int index = 0; index < numberOfSites; index++)
    {
      DNA.add(false);
    }
  }

  public Genome(ArrayList<Boolean> DNA)
   {
     this.DNA = DNA;
   }
   
   public Genome(ArrayList<Integer> activeSites, int NumberOfSites)
   {
     DNA = new ArrayList<Boolean>(NumberOfSites);
     Iterator<Integer> indexIterator = activeSites.iterator();
     while(indexIterator.hasNext())
     {
       Integer index = indexIterator.next();
       DNA.set(index, true);
     }
   }
   
   public static ArrayList<Genome> mergeGenomes(Genome first, Genome second, Genome master)
   {
     Random randomNumberGenerator = new Random();
     int randomNumber = randomNumberGenerator.nextInt(100);
     if(randomNumber <= mutationPercentage)
       mutate(first);
     randomNumber = randomNumberGenerator.nextInt(100);
     if(randomNumber <= mutationPercentage)
       mutate(second);
     int size = first.getSize();
     double part1 = new Integer(size).doubleValue() / 100.00;
     part1 = part1 * crossoverPercentage;
     int positionToCrossOver = (int) part1;

     List<Boolean> firstSectionFirstChild = first.getSection(0, positionToCrossOver);
     List<Boolean> firstSectionSecondChild = second.getSection(0, positionToCrossOver);
     List<Boolean> secondSectionFirstChild = second.getSection(positionToCrossOver, size);
     List<Boolean> secondSectionSecondChild = first.getSection(positionToCrossOver, size);
     
     //first child
     ArrayList<Boolean> firstChild = new ArrayList<Boolean>(size);
     firstChild.addAll(firstSectionFirstChild);
     firstChild.addAll(secondSectionFirstChild);
     Genome firstChildGenome = new Genome(firstChild);
     //second child
     ArrayList<Boolean> secondChild = new ArrayList<Boolean>(size);
     secondChild.addAll(firstSectionSecondChild);
     secondChild.addAll(secondSectionSecondChild);
     Genome secondChildGenome = new Genome(secondChild);
     //add to array, and return
     ArrayList<Genome> children = new ArrayList<Genome>();
     firstChildGenome = Genome.XorGenome(firstChildGenome, master);
     secondChildGenome = Genome.XorGenome(secondChildGenome, master);
     children.add(firstChildGenome);
     children.add(secondChildGenome);
     
     return children;
   }
   
  public static Genome XorGenome(Genome first, Genome master)
  {
    Iterator<Boolean> firstGeneIterator = first.geneIterator();
    Iterator<Boolean> masterGeneIterator = master.geneIterator();
    ArrayList<Boolean> newDNA = new ArrayList<Boolean>();
    
    while(firstGeneIterator.hasNext() || masterGeneIterator.hasNext())
    {
      Boolean firstGene = firstGeneIterator.next();
      Boolean mastergene = masterGeneIterator.next();
      if(firstGene || mastergene)
        newDNA.add(true);
      else
        newDNA.add(false);
    }
    return new Genome(newDNA);
  }

  private static void mutate(Genome clean)
  {
    Random randomGeneGenerator = new Random();
    int gene = randomGeneGenerator.nextInt(clean.getSize());
    Boolean value = clean.geneValue(gene);
    clean.replaceGene(gene, !value);
  }
  
  public Iterator<Boolean> geneIterator()
  {
    return this.DNA.iterator();
  }
  
  
  public boolean geneValue(int index)
  {
    return DNA.get(index);
  }
  
  public void replaceGene(int index, Boolean newValue)
  {
    this.DNA.set(index, newValue);
  }
  
  public int getSize()
  {
    return this.DNA.size();
  }
  
  public List<Boolean> getSection(int start, int finish)
  {
    ArrayList<Boolean> list = new ArrayList<Boolean>(finish - start);
    for(int index = start; index < finish; index ++)
    {
      list.add(this.DNA.get(index));
    }
    return list;
  }

  public void setFitness(double fitness)
  {
    this.fitness = fitness;
  }

  public double getFitness()
  {
    return fitness;
  }

  public void setEliteFitness(double eliteFitness)
  {
    this.eliteFitness = eliteFitness;
  }

  public double getEliteFitness()
  {
    return eliteFitness;
  }

  @Override
  public int compareTo(Genome o)
  {
    Double value = this.eliteFitness;
    Double otherValue = o.eliteFitness;
    return value.compareTo(otherValue);
  }
  
  @Override
  public boolean equals(Object o)
  {
    if(o instanceof Genome)
    {
      Genome other = (Genome) o;
      if(this.DNA.size() == other.DNA.size())
      {
        boolean different = false;
        for(int index =0; index < this.DNA.size(); index ++)
        {
          if(!different)
            if(this.DNA.get(index) != other.DNA.get(index))
              different = true;
        }
        return different;
      }
      else return false;
    }
    else
      return false;
  }

  public void setRt(RT rt)
  {
    this.rt = rt;
  }

  public RT getRt()
  {
    return rt;
  }
}
