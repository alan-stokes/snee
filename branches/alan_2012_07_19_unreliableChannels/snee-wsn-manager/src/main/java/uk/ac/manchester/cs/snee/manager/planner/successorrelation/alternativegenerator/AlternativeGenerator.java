package uk.ac.manchester.cs.snee.manager.planner.successorrelation.alternativegenerator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;

import com.rits.cloning.Cloner;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.WhenSchedulerException;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.AutonomicManagerComponent;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.localrepairstrategy.heursticsrouter.CandiateRouter;
import uk.ac.manchester.cs.snee.manager.planner.successorrelation.successor.Successor;
import uk.ac.manchester.cs.snee.manager.planner.successorrelation.tabu.TABUList;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;

public class AlternativeGenerator extends AutonomicManagerComponent
{

  private static final long serialVersionUID = -2274933349883496847L;
  private Successor successor;
  private File outputFolder;
  private SourceMetadataAbstract _metadata;
  private MetadataManager _metaManager;
  private ArrayList<String> failedNodes = new ArrayList<String>();
  private Topology top;
  /**
   * constructor
   * @param successor
   * @param topology 
   * @param _metadata 
   * @param _metaManager 
   */
  public AlternativeGenerator(File outputFolder, SourceMetadataAbstract _metadata, 
                              Topology top, MetadataManager _metaManager)
  {
	  this.manager = null;
	  this.outputFolder = outputFolder;
	  this._metadata = _metadata;
	  this._metaManager = _metaManager;
	  this.top = top;
  }
  
  public AlternativeGenerator(File outputFolder, SourceMetadataAbstract _metadata, 
                              Topology top,
                              MetadataManager _metaManager, ArrayList<String> failedNodes)
  {
    this.manager = null;
    this.outputFolder = outputFolder;
    this._metadata = _metadata;
    this._metaManager = _metaManager;
    this.failedNodes = failedNodes;
    this.top = top;
  }

  /**
   * generates a set of alternative qeps by using different decisions from the snee stack. 
   * @param aspirationPlusBounds 
   * @param tabuList 
   * @param position 
   * @return
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   * @throws OptimizationException 
   * @throws SchemaMetadataException 
   * @throws SNEEException 
   * @throws WhenSchedulerException 
   * @throws TypeMappingException 
   * @throws CodeGenerationException 
   * @throws IOException 
   */
  public ArrayList<Successor> generateAlternatives(Successor successor, 
                                                   int aspirationPlusBounds,
                                                   TABUList tabuList, int position) 
  throws NumberFormatException, SNEEConfigurationException, 
  SNEEException, SchemaMetadataException, OptimizationException, 
  WhenSchedulerException, TypeMappingException, IOException, CodeGenerationException
  {
    this.successor = successor;
    ArrayList<Successor> successors = new ArrayList<Successor>();
    //run hueristic router and then genetic router seeded with the huristic router.
    ArrayList<RT> candidateRoutes = HuristicRouter(this.successor.getQep().getDAF().getPAF());
    successors = GeneticRouter(candidateRoutes, tabuList, position);
    //add routes together
    return successors;
  }

  private ArrayList<RT> HuristicRouter(PAF paf) 
  throws NumberFormatException, SNEEConfigurationException
  {
    Topology top = reduceTopology();
    CandiateRouter metaRouter = 
      new CandiateRouter(top, outputFolder, 
                         this.successor.getQep().getDAF().getPAF(), this._metadata);
    return metaRouter.generateAlternativeRoutingTrees(this.successor.getQep().getQueryName());
  }

  private ArrayList<Successor> GeneticRouter(ArrayList<RT> candidateRoutes,
                                             TABUList tabuList, int position)
  throws IOException, SchemaMetadataException, TypeMappingException, OptimizationException, 
  CodeGenerationException
  {
    Topology top = reduceTopology();
    
    GeneticRouter geneticRouter = 
      new GeneticRouter(_metadata,top, this.successor.getQep().getIOT().getPAF(), 
                        outputFolder, tabuList, position);
    return geneticRouter.generateAlternativeRoutes(candidateRoutes, this.successor, _metaManager);  
  }

  private Topology reduceTopology()
  {
    Cloner cloner = new Cloner();
    cloner.dontClone(Logger.class);
    Topology toplogy = cloner.deepClone(top);
    Iterator<String> faieldNodeIterator = this.failedNodes.iterator();
    while(faieldNodeIterator.hasNext())
    {
      String failedNodeId = faieldNodeIterator.next();
      toplogy.removeNodeAndAssociatedEdges(failedNodeId);
    }
    return toplogy;
  }
} 
