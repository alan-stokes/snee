package uk.ac.manchester.cs.snee.manager.planner.overlaysuccessorrelation.alternativegenerator;

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
import uk.ac.manchester.cs.snee.manager.failednode.FailedNodeStrategyLocal;
import uk.ac.manchester.cs.snee.manager.planner.common.OverlaySuccessor;
import uk.ac.manchester.cs.snee.manager.planner.overlaysuccessorrelation.tabu.OverlayTABUList;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;

public class OverlayGeneticRouter extends AutonomicManagerComponent
{
  private static final long serialVersionUID = 1L;
  private ArrayList<OverlaySuccessor> eliteSolutions = new ArrayList<OverlaySuccessor>();
  private ArrayList<OverlayPhenome> elitePhenomes = new ArrayList<OverlayPhenome>();
  private ArrayList<String> nodeIds = new ArrayList<String>();
  private OverlayGenome requiredSites; 
  private Topology network;
  private static final int maxIterations = 50;
  private static final int populationSize = 30;
  private File geneicFolder = null;
  private OverlayGeneticRouterPhenomeFitness fitness;
  private OverlayTABUList tabuList;
  private int position;
  private int consecutiveTimesWithoutNewSolutions = 0;
  private static final int AllowedNumberOfIterationsWithoutNewSolution = 4;
  

  public OverlayGeneticRouter(SourceMetadataAbstract _metadata, Topology top, 
                       PAF paf, File plannerFile, OverlayTABUList tabuList, int position)
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
    requiredSites = new OverlayGenome(sink, sources, network.getNodes().size());
    nodeIds.addAll(network.getAllNodes().keySet());
    Collections.sort(nodeIds);
    
  }
  
  public ArrayList<OverlaySuccessor> generateAlternativeRoutes(ArrayList<RT> candidateRoutes, 
                                                   OverlaySuccessor qep, MetadataManager metamanager,
                                                   FailedNodeStrategyLocal localNodeFailureStrategy) 
  throws IOException, SchemaMetadataException, TypeMappingException,
  OptimizationException, CodeGenerationException
  {
    fitness = new OverlayGeneticRouterPhenomeFitness(qep, geneicFolder , metamanager, 
                                              network, nodeIds, _metadata, true);
    ArrayList<OverlayGenome> initalPopulation = 
      generateInitialPopulation(candidateRoutes, qep, localNodeFailureStrategy);
    int currentIteration = 0;
    ArrayList<OverlayGenome> currentPopulation = initalPopulation;
   
    while(!meetStoppingCriteria(currentIteration))
    {
      File iterationFolder = new File(geneicFolder.toString() + sep + "iteration" + currentIteration);
      iterationFolder.mkdir();
      currentPopulation = repopulate(currentPopulation, qep);
      locateAlternatives(currentPopulation, iterationFolder, qep, localNodeFailureStrategy);
      currentIteration++;
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
   * collects OverlaySuccessors from the best phenomes
   */
  private void collectSolutions()
  {
    Iterator<OverlayPhenome> eliteIterator = elitePhenomes.iterator();
    while(eliteIterator.hasNext())
    {
      OverlayPhenome elite = eliteIterator.next();
      eliteSolutions.add(elite.getOverlaySuccessor());
    }
  }

  /**
   * searches the current population to locate valid routing trees with time aspects
   * @param currentPopulation
   * @param iterationFolder
   * @param qep 
 * @param localNodeFailureStrategy 
   * @param lowEnergyThershold
   */
  private void locateAlternatives(ArrayList<OverlayGenome> currentPopulation, File iterationFolder,
                                  OverlaySuccessor qep, FailedNodeStrategyLocal localNodeFailureStrategy)
  {
    boolean addedNewSolution = false;
    Iterator<OverlayGenome> popIterator = currentPopulation.iterator();
    while(popIterator.hasNext())
    {
      OverlayGenome pop = popIterator.next();
      OverlayPhenome popPhenome = 
    	  this.fitness.determineFitness(pop, qep.getEstimatedLifetimeInAgendaCountBeforeSwitch() + qep.getPreviousAgendaCount(),
    			                        localNodeFailureStrategy, qep.getLogicalOverlayNetwork());
      pop.setFitness(popPhenome.getFitness());
      if(popPhenome.getFitness() == 1 && 
         !tabuList.isEntirelyTABU(popPhenome.getOverlaySuccessor().getQep(), position) &&
         !alreadyContainsRT(popPhenome))
      {
        if(elitePhenomes.size() == populationSize)
        {
          double eliteFitness = popPhenome.elitefitness();
          if(elitePhenomes.get(elitePhenomes.size()-1).elitefitness() > eliteFitness)
          {
            addedNewSolution = true; 
            addInCorrectPos(popPhenome);
            elitePhenomes = 
              new ArrayList<OverlayPhenome>(elitePhenomes.subList(0, elitePhenomes.size() -1));
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
  private void addInCorrectPos(OverlayPhenome popPhenome)
  {
    Iterator<OverlayPhenome> pehoneIterator = this.elitePhenomes.iterator();
    boolean found = false;
    int position = 0;
    while(!found && pehoneIterator.hasNext())
    {
      OverlayPhenome next = pehoneIterator.next();
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
  private Boolean alreadyContainsRT(OverlayPhenome pop)
  {
    RT rt = pop.getRt();
    Iterator<OverlayPhenome> eliteGenomeIterator = this.elitePhenomes.iterator();
    while(eliteGenomeIterator.hasNext())
    {
      OverlayPhenome elitePhenome = eliteGenomeIterator.next();
      RT eliteRT = elitePhenome.getRt();
      if(RT.equals(eliteRT, rt))
      {
        return true;
      }
    }
    return false;
  }

  /**
   * does the mutation and crossover
   * @param currentPopulation
   * @param OverlaySuccessor
   * @return
   */
  private ArrayList<OverlayGenome> repopulate(ArrayList<OverlayGenome> currentPopulation,
                                              OverlaySuccessor OverlaySuccessor)
  {
    ArrayList<OverlayGenome> newPop = new ArrayList<OverlayGenome>();
    ArrayList<OverlayGenome> makesTrees = new ArrayList<OverlayGenome>();
    ArrayList<OverlayGenome> failesToMakeTrees = new ArrayList<OverlayGenome>();
    Iterator<OverlayGenome> popIterator = currentPopulation.iterator();
    while(popIterator.hasNext())
    {
      OverlayGenome pop = popIterator.next();
      if(pop.getFitness() == 0)
        failesToMakeTrees.add(pop);
      else
        makesTrees.add(pop);
    }
    
    ArrayList<OverlayGenome> order = new ArrayList<OverlayGenome>(makesTrees);
    order.addAll(failesToMakeTrees);
    Iterator<OverlayGenome> orderedGenomes = order.iterator();
    Random random = new Random(new Long(0));
    while(newPop.size() < populationSize)
    {
      OverlayGenome first = null;
      OverlayGenome second = null;
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
      ArrayList<OverlayGenome> children = OverlayGenome.mergeGenomes(first, second, 
                                  requiredSites, OverlaySuccessor.getEstimatedLifetimeInAgendas());
      newPop.addAll(children); 
    }
   return newPop;
  }

  private  ArrayList<OverlayGenome> generateInitialPopulation(
                                    ArrayList<RT> candidateRoutes,
		                                OverlaySuccessor qep, 
		                                FailedNodeStrategyLocal localNodeFailureStrategy)
  {
    ArrayList<OverlayGenome> population = new ArrayList<OverlayGenome>();
    Iterator<RT> rtIterator = candidateRoutes.iterator();
    Random randomNumberGenerator = new Random(new Long(1));
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
      OverlayGenome newPop = new OverlayGenome(currentDNA);
      this.fitness.determineFitness(newPop, qep.getEstimatedLifetimeInAgendas(),
                                    localNodeFailureStrategy, qep.getLogicalOverlayNetwork());
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
      OverlayGenome newPop = new OverlayGenome(currentDNA);
      newPop = OverlayGenome.XorGenome(newPop, requiredSites);
      population.add(newPop);
    }
    return population;
  }
}
