package uk.ac.manchester.cs.snee.manager.planner.successorrelation.alternativegenerator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Random;

import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.common.graph.Tree;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.manager.common.AutonomicManagerComponent;
import uk.ac.manchester.cs.snee.manager.planner.common.Successor;
import uk.ac.manchester.cs.snee.manager.planner.successorrelation.tabu.TABUList;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;

public class GeneticRouter extends AutonomicManagerComponent
{
  private static final long serialVersionUID = 1L;
  private ArrayList<Successor> eliteSolutions = new ArrayList<Successor>();
  private ArrayList<Phenome> elitePhenomes = new ArrayList<Phenome>();
  private ArrayList<String> nodeIds = new ArrayList<String>();
  private Genome requiredSites; 
  private Topology network;
  private static final int maxIterations = 50;
  private static final int populationSize = 30;
  private File geneicFolder = null;
  private GeneticRouterFitness fitness;
  private TABUList tabuList;
  private int position;
  private int consecutiveTimesWithoutNewSolutions = 0;
  private static final int AllowedNumberOfIterationsWithoutNewSolution = 4;
  

  public GeneticRouter(SourceMetadataAbstract _metadata, Topology top, 
                       PAF paf, File plannerFile, TABUList tabuList, int position)
  {
    geneicFolder = new File(plannerFile.toString() + sep + "geneticRouter");
    geneicFolder.mkdir();
    this.tabuList = tabuList;
    this.position = position;
    this._metadata = _metadata;
    SensorNetworkSourceMetadata sm = (SensorNetworkSourceMetadata) _metadata;
    network = top;
    int sink = sm.getGateway(); 
    int[] sources = sm.getSourceSites(paf);
    requiredSites = new Genome(sink, sources, network.getNodes().size(), 0);
    nodeIds.addAll(network.getAllNodes().keySet());
    Collections.sort(nodeIds);
    
  }
  
  public ArrayList<Successor> generateAlternativeRoutes(ArrayList<RT> candidateRoutes, 
                                                   Successor qep, MetadataManager metamanager) 
  throws IOException, SchemaMetadataException, TypeMappingException,
  OptimizationException, CodeGenerationException
  {
    fitness = new GeneticRouterFitness(qep, geneicFolder , metamanager, network, nodeIds, _metadata, true);
    ArrayList<Genome> initalPopulation = generateInitialPopulation(candidateRoutes, qep);
    int currentIteration = 0;
    ArrayList<Genome> currentPopulation = initalPopulation;
   
    while(!meetStoppingCriteria(currentIteration))
    {
      File iterationFolder = new File(geneicFolder.toString() + sep + "iteration" + currentIteration);
      iterationFolder.mkdir();
      System.out.println("repop");
      currentPopulation = repopulate(currentPopulation, qep);
      System.out.println("locate");
      locateAlternatives(currentPopulation, iterationFolder, qep);
      currentIteration++;
      System.out.println("Now starting genetic router iteration " + currentIteration);
    }
    
    collectSolutions();
    return eliteSolutions;
  }

  /**
   * verifies when to stop searching
   * @param currentIteration
   * @return
   */
  private Boolean meetStoppingCriteria(int currentIteration)
  {
    if(currentIteration > maxIterations)
      return true;
    else
    {
      if(elitePhenomes.size() > 0)
      {
        if(consecutiveTimesWithoutNewSolutions <= AllowedNumberOfIterationsWithoutNewSolution)
          return false;
        else
          return true;
      }
      else
        return false;
    }
  }

  /**
   * collects successors from the best phenomes
   */
  private void collectSolutions()
  {
    Iterator<Phenome> eliteIterator = elitePhenomes.iterator();
    while(eliteIterator.hasNext())
    {
      Phenome elite = eliteIterator.next();
      eliteSolutions.add(elite.getSuccessor());
    }
  }

  /**
   * searches the current population to locate valid routing trees with time aspects
   * @param currentPopulation
   * @param iterationFolder
   * @param qep 
   * @param lowEnergyThershold
   */
  private void locateAlternatives(ArrayList<Genome> currentPopulation, File iterationFolder,
                                  Successor qep)
  {
    boolean addedNewSolution = false;
    Iterator<Genome> popIterator = currentPopulation.iterator();
    while(popIterator.hasNext())
    {
      Genome pop = popIterator.next();
      Phenome popPhenome = this.fitness.determineFitness(pop, qep.getAgendaCount() + qep.getPreviousAgendaCount());
      pop.setFitness(popPhenome.getFitness());
      if(popPhenome.getFitness() == 1)
      {
        if(!tabuList.isEntirelyTABU(popPhenome.getSuccessor().getQep(), position))
        {
          if(!alreadyContainsRT(popPhenome.getRt()))
          {
            if(elitePhenomes.size() == populationSize)
            {
              double eliteFitness = popPhenome.elitefitness();
              if(elitePhenomes.get(elitePhenomes.size()-1).elitefitness() > eliteFitness)
              {
                addedNewSolution = true; 
                addInCorrectPos(popPhenome);
                elitePhenomes = new ArrayList<Phenome>(elitePhenomes.subList(0, elitePhenomes.size() -1));
              } 
            }
            else
            {
              addedNewSolution = true;
              elitePhenomes.add(popPhenome);
              addInCorrectPos(popPhenome);
              //Collections.sort(elitePhenomes);
            }
          }
        }
      }
    }
    if(!addedNewSolution)
    {
      consecutiveTimesWithoutNewSolutions++;
    }
    else
    {
      consecutiveTimesWithoutNewSolutions = 0;
    }
  }

  /**
   * used to speed up system. faster than collections.sort
   * @param popPhenome
   */
  private void addInCorrectPos(Phenome popPhenome)
  {
    Iterator<Phenome> pehoneIterator = this.elitePhenomes.iterator();
    boolean found = false;
    int position = 0;
    while(!found && pehoneIterator.hasNext())
    {
      Phenome next = pehoneIterator.next();
      if(next.elitefitness() <= popPhenome.elitefitness())
        found = true;
      else
        position++;
    }
    elitePhenomes.add(position, popPhenome);
  }

  /**
   * helper method which checks if a solution of a genome is already in the elite genomes area
   * @param rt
   * @return
   */
  private boolean alreadyContainsRT(RT rt)
  {
    Iterator<Phenome> eliteGenomeIterator = this.elitePhenomes.iterator();
    while(eliteGenomeIterator.hasNext())
    {
      Phenome elitePhenome = eliteGenomeIterator.next();
      RT eliteRT = elitePhenome.getRt();
      if(RT.equals(eliteRT, rt))
        return true;
    }
    return false;
  }

  /**
   * does the mutation and crossover
   * @param currentPopulation
   * @param successor
   * @return
   */
  private ArrayList<Genome> repopulate(ArrayList<Genome> currentPopulation, Successor successor)
  {
    ArrayList<Genome> newPop = new ArrayList<Genome>();
    ArrayList<Genome> makesTrees = new ArrayList<Genome>();
    ArrayList<Genome> failesToMakeTrees = new ArrayList<Genome>();
    Iterator<Genome> popIterator = currentPopulation.iterator();
    while(popIterator.hasNext())
    {
      Genome pop = popIterator.next();
      if(pop.getFitness() == 0)
        failesToMakeTrees.add(pop);
      else
        makesTrees.add(pop);
    }
    
    ArrayList<Genome> order = new ArrayList<Genome>(makesTrees);
    order.addAll(failesToMakeTrees);
    Iterator<Genome> orderedGenomes = order.iterator();
    Random random = new Random();
    while(newPop.size() < populationSize)
    {
      Genome first = null;
      Genome second = null;
      if(orderedGenomes.hasNext())
        first = orderedGenomes.next();
      else
      {
        int randomIndex = random.nextInt(order.size());
        first = order.get(randomIndex);
      }
      if(orderedGenomes.hasNext())
        second = orderedGenomes.next();
      else
      {
        int randomIndex = random.nextInt(order.size());
        second = order.get(randomIndex);
      }
      ArrayList<Genome> children = Genome.mergeGenomes(first, second, 
                                  requiredSites, successor.getLifetimeInAgendas());
      newPop.addAll(children); 
    }
   return newPop;
  }

  private  ArrayList<Genome> generateInitialPopulation(ArrayList<RT> candidateRoutes,
		                                                   Successor qep)
  {
    ArrayList<Genome> population = new ArrayList<Genome>();
    Iterator<RT> rtIterator = candidateRoutes.iterator();
    Random randomNumberGenerator = new Random();
    while(rtIterator.hasNext() && population.size() < populationSize)
    {
      RT currentRT = rtIterator.next();
      Tree curenttree = currentRT.getSiteTree();
      ArrayList<Boolean> currentDNA = new ArrayList<Boolean>();
      for(int index = 0; index < this.nodeIds.size(); index ++)
      {
        Node node = network.getNode(this.nodeIds.get(index));
        if(node != null)
        {
          if(curenttree.getNode(node.getID()) != null)
            currentDNA.add(true);
          else
            currentDNA.add(false);
        }   
      }
      int agendaTime = randomNumberGenerator.nextInt(qep.getLifetimeInAgendas());
      Genome newPop = new Genome(currentDNA, agendaTime);
      this.fitness.determineFitness(newPop, qep.getLifetimeInAgendas());
      population.add(newPop);
    }
    while(population.size() < populationSize)
    {
      ArrayList<Boolean> currentDNA = new ArrayList<Boolean>();
      for(int index = 0; index < this.nodeIds.size(); index ++)
      {
        int randomNumber = randomNumberGenerator.nextInt(100);
        if(randomNumber > 10)
          currentDNA.add(true);
        else
          currentDNA.add(false);
      }
      int agendaTime = randomNumberGenerator.nextInt(qep.getLifetimeInAgendas());
      Genome newPop = new Genome(currentDNA, agendaTime);
      newPop = Genome.XorGenome(newPop, requiredSites);
      population.add(newPop);
    }
    return population;
  }
}
