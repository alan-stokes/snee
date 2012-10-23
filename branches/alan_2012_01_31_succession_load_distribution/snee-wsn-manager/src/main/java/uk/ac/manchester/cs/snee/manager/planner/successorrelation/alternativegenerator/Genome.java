package uk.ac.manchester.cs.snee.manager.planner.successorrelation.alternativegenerator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class Genome 
{
   private ArrayList<Boolean> DNA;
   private GenomeTimeBitMap timingMapping = null;
   private int mappingValue = 0;
   private final static int crossoverPercentage = 90;
   private final static int mutationPercentage = 2;
   private final static int timeMutationPercentage = 50;
   private final static double mean = 0.0;
   private final static double variance = 0.001;
   private double fitness;
   private static Random randomGeneGenerator = new Random(new Long(0));
  
   //set of constructors
   public Genome(int sink, int [] sources, int NumberOfSites, GenomeTimeBitMap mapping, int mapValue)
   {
     DNA = new ArrayList<Boolean>(NumberOfSites);
     fillDNA(NumberOfSites);
     DNA.set(sink, true);
     for(int index = 0; index < sources.length; index++)
     {
       DNA.set(sources[index], true);
     }
     this.timingMapping = mapping;
     this.mappingValue = mapValue;
   }

  public Genome(ArrayList<Boolean> DNA, GenomeTimeBitMap mapping, int mapValue)
  {
     this.DNA = DNA;
     this.timingMapping = mapping;
     this.mappingValue = mapValue;
  }
   
   //helper constructor method
   private void fillDNA(int numberOfSites)
   {
     for(int index = 0; index < numberOfSites; index++)
     {
       DNA.add(false);
     }
   }
   
   /**
    * takes two genotypes and merges them to generate new offspring. using basic crossover and mutation
    * @param first
    * @param second
    * @param master
    * @return
    */
   public static ArrayList<Genome> mergeGenomes(Genome first, Genome second, Genome master, int successorLifetime)
   {
     Random randomNumberGenerator = new Random(new Long(0));
     int randomNumber = randomNumberGenerator.nextInt(100);
     if(randomNumber <= mutationPercentage)
       mutate(first);
     randomNumber = randomNumberGenerator.nextInt(100);
     if(randomNumber <= mutationPercentage)
       mutate(second);
     randomNumber = randomNumberGenerator.nextInt(100);
     if(randomNumber <= timeMutationPercentage)
       mutateTime(first, successorLifetime);
     randomNumber = randomNumberGenerator.nextInt(100);
     if(randomNumber <= timeMutationPercentage)
       mutateTime(second, successorLifetime);
       
     int size = first.getSize();
     ArrayList<Boolean> firstChild = new ArrayList<Boolean>(size);
     ArrayList<Boolean> secondChild = new ArrayList<Boolean>(size);
     firstChild.addAll(first.getDNA());
     secondChild.addAll(second.getDNA());
     
     for(int firstChildIndex = 0; firstChildIndex < size; firstChildIndex++)
     {
       if(randomGeneGenerator.nextBoolean())
       {
         Boolean state = firstChild.get(firstChildIndex);
         firstChild.set(firstChildIndex, secondChild.get(firstChildIndex));
         secondChild.set(firstChildIndex, state);
       }
     }
     
     for(int secondChildIndex = 0; secondChildIndex < size; secondChildIndex++)
     {
       if(randomGeneGenerator.nextBoolean())
       {
         Boolean state = secondChild.get(secondChildIndex);
         secondChild.set(secondChildIndex, firstChild.get(secondChildIndex));
         firstChild.set(secondChildIndex, state);
       }
     }
     
     Genome secondChildGenome = null;
     Genome firstChildGenome = null;
     
     if(randomGeneGenerator.nextBoolean())
     {
       secondChildGenome = new Genome(secondChild, first.getBitMapping(), first.getMappingValue());
       firstChildGenome = new Genome(firstChild, second.getBitMapping(), second.getMappingValue());
     }
     else
     {
       secondChildGenome = new Genome(secondChild, second.getBitMapping(), second.getMappingValue());
       firstChildGenome = new Genome(firstChild, first.getBitMapping(), first.getMappingValue());
     }
     
     //add to array, and return
     ArrayList<Genome> children = new ArrayList<Genome>();
     firstChildGenome = Genome.XorGenome(firstChildGenome, master);
     secondChildGenome = Genome.XorGenome(secondChildGenome, master);
     children.add(firstChildGenome);
     children.add(secondChildGenome);
     
     return children;
   }
   
  /**
   * helper method to keep sure the genotype router section has the correct Steiner nodes.
   * @param first
   * @param master
   * @return
   */
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
    return new Genome(newDNA, first.getBitMapping(), first.mappingValue);
  }

  /**
   * masic mutation function
   * @param clean
   */
  private static void mutate(Genome clean)
  {
    int gene = randomGeneGenerator.nextInt(clean.getSize());
    Boolean value = clean.geneValue(gene);
    clean.replaceGene(gene, !value);
  }
  
  /**
   * mutation aspect for time
   * @param clean
   * @param successorLifetime
   *//*
  public static void mutateTime(Genome clean, int successorLifetime)
  {
    
    
    clean.setTimeMap(randomGeneGenerator.nextInt(clean.getBitMapping().getMaxSegments()));
  }*/
  
  /**
   * mutation aspect for time using gaussian scope
   * @param clean
   * @param successorLifetime
   */
  public static void mutateTime(Genome clean, int successorLifetime)
  {
    
    
    clean.setTimeMap(getGaussian(Genome.mean, successorLifetime));
  }
  
  private static int getGaussian(double mean, int variance)
  {
    Double time = new Double (-1);
    while(time < 0 || time > variance)
      time=  new Double(mean + new Random().nextGaussian() * variance);
    return time.intValue();
  }

  
  /**
   * sets the mapping value
   * @param nextInt
   */
  private void setTimeMap(int mapping)
  {
    this.mappingValue = mapping;
  }
  
  /**
   * returns the genomes mapping value for the bit map
   * @return
   */
  public int getMappingValue()
  {
    return this.mappingValue;
  }

  /**
   * gene iterator
   * @return
   */
  public Iterator<Boolean> geneIterator()
  {
    return this.DNA.iterator();
  }
  
  /**
   * get value of a specific gene
   * @param index
   * @return
   */
  public boolean geneValue(int index)
  {
    return DNA.get(index);
  }
  
  /**
   * replace a specific gene
   * @param index
   * @param newValue
   */
  public void replaceGene(int index, Boolean newValue)
  {
    this.DNA.set(index, newValue);
  }
  
  /**
   * get number of genes in the DNA
   * @return
   */
  public int getSize()
  {
    return this.DNA.size();
  }
  
  /**
   * helper method to slice a section of DNA
   * @param start
   * @param finish
   * @return
   */
  public List<Boolean> getSection(int start, int finish)
  {
    ArrayList<Boolean> list = new ArrayList<Boolean>(finish - start);
    for(int index = start; index < finish; index ++)
    {
      list.add(this.DNA.get(index));
    }
    return list;
  }

  /**
   * set fitness of genotype
   * @param fitness
   */
  public void setFitness(double fitness)
  {
    this.fitness = fitness;
  }

  /**
   * get fitness of genotype
   * @return
   */
  public double getFitness()
  {
    return fitness;
  }
  
  /**
   * returns time period of genome
   * @return
   */
  public int getTimePeriod()
  {
    return mappingValue;
  }
  
  /**
   * returns the mapping for times
   * @return
   */
  public GenomeTimeBitMap getBitMapping()
  {
    return this.timingMapping;
  }
  
  /**
   * changes the mapping value of the genome
   */
  public void setMappingValue(int genomeMappingValue)
  {
    this.mappingValue = genomeMappingValue; 
  }
  
  /**
   * compares 2 genomes to determine if they are equal
   */
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
  
  public String toString()
  {
    String form = "";
    Iterator<Boolean> dnaIterator = this.DNA.iterator();
    while(dnaIterator.hasNext())
    {
      boolean active = dnaIterator.next();
      if(active)
        form = form.concat("1 ");
      else
        form = form.concat("0 ");
    }
    form = form.concat(new Long(this.mappingValue).toString());
    return form;
  }
  
  public ArrayList<Boolean> getDNA()
  {
    return this.DNA;
  }
}
