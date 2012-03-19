package uk.ac.manchester.cs.snee.manager.planner.successorrelation.alternativegenerator;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Random;
import java.util.logging.Logger;

import com.rits.cloning.Cloner;

import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.common.graph.Tree;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.RTUtils;
import uk.ac.manchester.cs.snee.compiler.sn.router.Router;
import uk.ac.manchester.cs.snee.manager.common.AutonomicManagerComponent;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.RadioLink;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyUtils;

public class GeneticRouter extends AutonomicManagerComponent
{
  private static final long serialVersionUID = 1L;
  private ArrayList<Tree> eliteSolutions = new ArrayList<Tree>();
  private ArrayList<Genome> eliteGenomes = new ArrayList<Genome>();
  private ArrayList<String> nodeIds = new ArrayList<String>();
  private Genome requiredSites; 
  private Topology network;
  private static final int maxIterations = 100;
  private static final int populationSize = 30;
  private PAF paf;
  private File geneicFolder = null;
  

  public GeneticRouter(SourceMetadataAbstract _metadata, Topology top, 
                       PAF paf, File plannerFile)
  {
    geneicFolder = new File(plannerFile.toString() + sep + "geneticRouter");
    geneicFolder.mkdir();
    this._metadata = _metadata;
    this.paf = paf;
    SensorNetworkSourceMetadata sm = (SensorNetworkSourceMetadata) _metadata;
    network = top;
    int sink = sm.getGateway(); 
    int[] sources = sm.getSourceSites(paf);
    
    requiredSites = new Genome(sink, sources, network.getNodes().size());
    nodeIds.addAll(network.getAllNodes().keySet());
    Collections.sort(nodeIds);
    
  }
  
  public ArrayList<Tree> generateAlternativeRoutes(ArrayList<RT> candidateRoutes)
  {
    ArrayList<Genome> initalPopulation = generateInitialPopulation(candidateRoutes);
    int currentIteration = 0;
    ArrayList<Genome> currentPopulation = initalPopulation;
   
    while(currentIteration < maxIterations)
    {
      File iterationFolder = new File(geneicFolder.toString() + sep + "iteration" + currentIteration);
      iterationFolder.mkdir();
      currentPopulation = repopulate(currentPopulation);
      locateAlternatives(currentPopulation, iterationFolder);
      currentIteration++;
      System.out.println("Now starting genetic router iteration " + currentIteration);
    }
    
    generateTrees();
    return eliteSolutions;
  }

  private void generateTrees()
  {
    Iterator<Genome> eliteIterator = eliteGenomes.iterator();
    while(eliteIterator.hasNext())
    {
      Genome elite = eliteIterator.next();
      eliteSolutions.add(elite.getRt().getSiteTree());
    }
  }

  private void locateAlternatives(ArrayList<Genome> currentPopulation, File iterationFolder)
  {
    Iterator<Genome> popIterator = currentPopulation.iterator();
    while(popIterator.hasNext())
    {
      Genome pop = popIterator.next();
      double fitness = this.fitness(pop);
      pop.setFitness(fitness);
      if(fitness == 1)
      {
        double eliteFitness = this.elitefitness(pop);
        if(!alreadyContainsRT(pop.getRt()))
        {
          if(eliteGenomes.size() == populationSize)
          {
            if(eliteGenomes.get(eliteGenomes.size()-1).getEliteFitness() > eliteFitness)
            {
              eliteGenomes.set(eliteGenomes.size()-1, pop);
              Collections.sort(eliteGenomes);
            } 
          }
          else
          {
            eliteGenomes.add(pop);
            Collections.sort(eliteGenomes);
          }
        }
      }
    }
    Iterator<Genome> eliteIterator = eliteGenomes.iterator();
    int id = 1;
    while(eliteIterator.hasNext())
    {
      Genome eliteGen = eliteIterator.next();
      new RTUtils(eliteGen.getRt()).exportAsDotFile(iterationFolder + sep + "elite" + id);
      id ++;
    }
  }

  private boolean alreadyContainsRT(RT rt)
  {
    Iterator<Genome> eliteGenomeIterator = this.eliteGenomes.iterator();
    while(eliteGenomeIterator.hasNext())
    {
      Genome eliteGenome = eliteGenomeIterator.next();
      RT eliteRT = eliteGenome.getRt();
      if(areTheSame(eliteRT, rt))
        return true;
    }
    return false;
  }

  private boolean areTheSame(RT eliteRT, RT rt)
  {
    Tree template = eliteRT.getSiteTree();
    Tree compare = rt.getSiteTree();
    ArrayList<Node> templateNodes = new ArrayList<Node>(template.getNodes());
    ArrayList<Node> compareNodes = new ArrayList<Node>(compare.getNodes());
    if(templateNodes.size() == compareNodes.size())
    {
      Iterator<Node> templateIterator = templateNodes.iterator();
      Iterator<Node> compareIterator = compareNodes.iterator();
      while(templateIterator.hasNext())
      {
        Node currentNode = templateIterator.next();
        if(!compareNodeToArray(currentNode, compareNodes))
          return false;        
      }
      while(compareIterator.hasNext())
      {
        Node currentNode = compareIterator.next();
        if(!compareNodeToArray(currentNode, templateNodes))
          return false;
      }
      return true;
    }
    return false;
  }

  
  private boolean compareNodeToArray(Node currentNode,
      ArrayList<Node> compareNodes)
  {
    for(int index = 0; index < compareNodes.size(); index++)
    {
      if(currentNode.getID().equals(compareNodes.get(index).getID()))
        return true;
    }
    return false;
  }

  private ArrayList<Genome> repopulate(ArrayList<Genome> currentPopulation)
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
      ArrayList<Genome> children = Genome.mergeGenomes(first, second, requiredSites);
      newPop.addAll(children); 
    }
   return newPop;
  }

  private  ArrayList<Genome> generateInitialPopulation(ArrayList<RT> candidateRoutes)
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
      Genome newPop = new Genome(currentDNA);
      newPop.setFitness(fitness(newPop));
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
      Genome newPop = new Genome(currentDNA);
      newPop = Genome.XorGenome(newPop, requiredSites);
      population.add(newPop);
    }
    return population;
  }

  private double fitness(Genome newPop)
  {
    Cloner cloner = new Cloner();
    cloner.dontClone(Logger.class);
    
    Topology currentTopology = cloner.deepClone(network);
    
    int counter = 0;
    Iterator<Boolean> geneIterator = newPop.geneIterator();
    while(geneIterator.hasNext())
    {
      Boolean geneValue = geneIterator.next();
      if(!geneValue)
      {
        currentTopology.removeNode(this.nodeIds.get(counter));
        currentTopology.removeAssociatedEdges(this.nodeIds.get(counter));
      }
      counter ++;
    }
    try
    {
      new TopologyUtils(currentTopology).exportAsDOTFile(this.geneicFolder.toString() + sep + "currentTop", false);
      Router router = new Router();
      RT rt = router.doRouting(this.paf, "", currentTopology, _metadata);
      newPop.setRt(rt);
      return 1;
    }
    catch(Exception e)
    {
    //  e.printStackTrace();
      return 0;
    }
  }
  
  private double elitefitness(Genome newPop)
  {
    RT route = newPop.getRt();
    Iterator<Node> nodeIterator = route.getSiteTree().getNodes().iterator();
    double overallCost = 0.0;
    while(nodeIterator.hasNext())
    {
      Node node = nodeIterator.next();
      if(node.getOutDegree() != 0)
      {
        Node output = node.getOutput(0);
        RadioLink link = route.getRadioLink((Site)node, (Site)output);
        overallCost += link.getEnergyCost();
      }
    }
    return overallCost;
  }
}
