package uk.ac.manchester.cs.snee.manager.planner.overlaysuccessorrelation.alternativegenerator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.WhenSchedulerException;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.AutonomicManagerComponent;
import uk.ac.manchester.cs.snee.manager.failednode.FailedNodeStrategyLocal;
import uk.ac.manchester.cs.snee.manager.failednode.alternativerouter.CandiateRouter;
import uk.ac.manchester.cs.snee.manager.planner.common.OverlaySuccessor;
import uk.ac.manchester.cs.snee.manager.planner.overlaysuccessorrelation.tabu.OverlayTABUList;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;

public class OverlayAlternativeGenerator extends AutonomicManagerComponent
{

  private static final long serialVersionUID = -2274933349883496847L;
  private OverlaySuccessor successor;
  private File outputFolder;
  private SourceMetadataAbstract _metadata;
  private MetadataManager _metaManager;
  /**
   * constructor
   * @param successor
   * @param topology 
   * @param _metadata 
   * @param _metaManager 
   */
  public OverlayAlternativeGenerator(File outputFolder, SourceMetadataAbstract _metadata, 
                              AutonomicManagerImpl manager, MetadataManager _metaManager)
  {
	  this.manager = manager;
	  this.outputFolder = outputFolder;
	  this._metadata = _metadata;
	  this._metaManager = _metaManager;
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
  public ArrayList<OverlaySuccessor> generateAlternatives(OverlaySuccessor successor, 
                                                   int aspirationPlusBounds,
                                                   OverlayTABUList tabuList, int position) 
  throws NumberFormatException, SNEEConfigurationException, 
  SNEEException, SchemaMetadataException, OptimizationException, 
  WhenSchedulerException, TypeMappingException, IOException, CodeGenerationException
  {
    this.successor = successor;
    ArrayList<OverlaySuccessor> successors = new ArrayList<OverlaySuccessor>();
    //run hueristic router and then genetic router seeded with the huristic router.
    ArrayList<RT> candidateRoutes = HuristicRouter(this.successor.getQep().getDAF().getPAF());
    successors = GeneticRouter(candidateRoutes, tabuList, position);
    //add routes together
    return successors;
  }

  private ArrayList<RT> HuristicRouter(PAF paf) 
  throws NumberFormatException, SNEEConfigurationException
  {
    CandiateRouter metaRouter = 
      new CandiateRouter(this.manager.getWsnTopology(), outputFolder, 
                         this.successor.getQep().getDAF().getPAF(), this._metadata);
    return metaRouter.generateAlternativeRoutingTrees(this.successor.getQep().getQueryName());
  }

  private ArrayList<OverlaySuccessor> GeneticRouter(ArrayList<RT> candidateRoutes,
                                             OverlayTABUList tabuList, int position)
  throws IOException, SchemaMetadataException, TypeMappingException, OptimizationException, 
  CodeGenerationException
  {
	FailedNodeStrategyLocal localNodeFailureStrategy = new FailedNodeStrategyLocal(manager, _metadata, _metaManager);
    OverlayGeneticRouter geneticRouter = 
      new OverlayGeneticRouter(_metadata,this.manager.getWsnTopology(), this.successor.getQep().getIOT().getPAF(), 
                        outputFolder, tabuList, position);
    return geneticRouter.generateAlternativeRoutes(candidateRoutes, this.successor, _metaManager, localNodeFailureStrategy);  
  }
} 
