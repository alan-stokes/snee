package uk.ac.manchester.cs.snee.manager.planner.overlaysuccessorrelation.alternativegenerator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class OverlayGenome 
{
   private ArrayList<Boolean> DNA;
   private final static int crossoverPercentage = 90;
   private final static int mutationPercentage = 2;
   private double fitness;
   private static Random randomGeneGenerator = new Random(new Long(0));
  
   //set of constructors
   public OverlayGenome(int sink, int [] sources, int NumberOfSites)
   {
     DNA = new ArrayList<Boolean>(NumberOfSites);
     fillDNA(NumberOfSites);
     DNA.set(sink, true);
     for(int index = 0; index < sources.length; index++)
     {
       DNA.set(sources[index], true);
     }
   }

  public OverlayGenome(ArrayList<Boolean> DNA)
  {
     this.DNA = DNA;
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
   public static ArrayList<OverlayGenome> mergeGenomes(OverlayGenome first, OverlayGenome second, OverlayGenome master, int successorLifetime)
   {
     Random randomNumberGenerator = new Random(new Long(0));
     int randomNumber = randomNumberGenerator.nextInt(100);
     if(randomNumber <= mutationPercentage)
       mutate(first);
     randomNumber = randomNumberGenerator.nextInt(100);
     if(randomNumber <= mutationPercentage)
       mutate(second);
     randomNumber = randomNumberGenerator.nextInt(100);
       
     int size = first.getSize();
     double part1 = new Integer(size).doubleValue() / 100.00;
     part1 = part1 * crossoverPercentage;
     int positionToCrossOver = (int) part1;
     //get sections of DNA
     List<Boolean> firstSectionFirstChild = first.getSection(0, positionToCrossOver);
     List<Boolean> firstSectionSecondChild = second.getSection(0, positionToCrossOver);
     List<Boolean> secondSectionFirstChild = second.getSection(positionToCrossOver, size);
     List<Boolean> secondSectionSecondChild = first.getSection(positionToCrossOver, size);
     
     //first child
     ArrayList<Boolean> firstChild = new ArrayList<Boolean>(size);
     firstChild.addAll(firstSectionFirstChild);
     firstChild.addAll(secondSectionFirstChild);
     OverlayGenome firstChildGenome = new OverlayGenome(firstChild);
     //second child
     ArrayList<Boolean> secondChild = new ArrayList<Boolean>(size);
     secondChild.addAll(firstSectionSecondChild);
     secondChild.addAll(secondSectionSecondChild);
     OverlayGenome secondChildGenome = new OverlayGenome(secondChild);
     //add to array, and return
     ArrayList<OverlayGenome> children = new ArrayList<OverlayGenome>();
     firstChildGenome = OverlayGenome.XorGenome(firstChildGenome, master);
     secondChildGenome = OverlayGenome.XorGenome(secondChildGenome, master);
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
  public static OverlayGenome XorGenome(OverlayGenome first, OverlayGenome master)
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
    return new OverlayGenome(newDNA);
  }

  /**
   * masic mutation function
   * @param clean
   */
  private static void mutate(OverlayGenome clean)
  {
    int gene = randomGeneGenerator.nextInt(clean.getSize());
    Boolean value = clean.geneValue(gene);
    clean.replaceGene(gene, !value);
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
   * compares 2 genomes to determine if they are equal
   */
  @Override
  public boolean equals(Object o)
  {
    if(o instanceof OverlayGenome)
    {
      OverlayGenome other = (OverlayGenome) o;
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
    return form;
  }
}
