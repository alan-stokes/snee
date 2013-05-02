package uk.ac.manchester.cs.snee.manager.planner.successorrelation;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.AdaptationUtils;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.common.StrategyAbstract;
import uk.ac.manchester.cs.snee.manager.common.StrategyIDEnum;
import uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.ChoiceAssessor;
import uk.ac.manchester.cs.snee.manager.planner.successorrelation.successor.Successor;
import uk.ac.manchester.cs.snee.manager.planner.successorrelation.successor.SuccessorPath;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;

public class AdaptationMonitor
{

  public static void 
  outputAdaptationDataBetweenSuccessors(SuccessorPath successor,
                                        SourceMetadataAbstract _metadata, 
                                        MetadataManager _metadataManager,
                                        File outputFolder, Topology network,
                                        HashMap<String, RunTimeSite> runningSites,
                                        CostParameters costs)
  throws IOException, OptimizationException, SchemaMetadataException, 
  TypeMappingException, CodeGenerationException
  {
    Iterator<Successor> successorIterator = successor.getSuccessorList().iterator();
    Successor first = successorIterator.next();
    int coutner = 1;
    while(successorIterator.hasNext())
    {
      Successor second = successorIterator.next();
      ChoiceAssessor assessor =
        new ChoiceAssessor(_metadata, _metadataManager, outputFolder, network);
      Adaptation adapt = 
        StrategyAbstract.generateAdaptationObject(first.getQep(), second.getQep());
      assessor.assessChoice(adapt, runningSites, true);
      AdaptationUtils utils = new AdaptationUtils(adapt, costs);
      utils.FileOutput(outputFolder, coutner);
      coutner++;
      first = second;
    }
  }
  
}
