package uk.ac.manchester.cs.snee.manager.planner;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.manager.Adaptation;
import uk.ac.manchester.cs.snee.manager.AdaptationUtils;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
public class ChoiceAssessor
{
  private String sep = System.getProperty("file.separator");
  private File AssessmentFolder;
  private MetadataManager _metadataManager;
  private SensorNetworkSourceMetadata _metadata;
  private Topology network;
  private File outputFolder;
  
  public ChoiceAssessor(SourceMetadataAbstract _metadata, MetadataManager _metadataManager,
                        File outputFolder)
  {
    this._metadataManager = _metadataManager;
    this._metadata = (SensorNetworkSourceMetadata) _metadata;
    network = this._metadata.getTopology();
    this.outputFolder = outputFolder;
  }

  public Adaptation assessChoices(List<Adaptation> choices) throws IOException
  {
    AssessmentFolder = new File(outputFolder.toString() + sep + "assessment");
    AssessmentFolder.mkdir();
    
    System.out.println("Starting assessment of choices");
    Iterator<Adaptation> choiceIterator = choices.iterator();
    while(choiceIterator.hasNext())
    {
      Adaptation adapt = choiceIterator.next();
      adapt.setEnergyCost(this.energyCost(adapt));
      adapt.setTimeCost(this.energyCost(adapt));
    }
    new AdaptationUtils(choices, _metadataManager.getCostParameters()).FileOutput(AssessmentFolder);
    return this.locateBestAdaptation(choices);
  }
  /**
   * Method which determines the energy cost of making the adaptation.
   * @param adapt
   * @return returns the cost in energy units.
   */
  private Long energyCost(Adaptation adapt)
  {
    return (long) 0;
  }
  
  /**
   * Method which determines the time cost of an adaptation
   * @param adapt
   * @return
   */
  private Long timeCost(Adaptation adapt)
  {
    return (long) 0;
  }
  
  /**
   * goes though the choices list locating the one with the least energy and time costs.
   * @param choices
   * @return
   */
  private Adaptation locateBestAdaptation(List<Adaptation> choices)
  {
    List<Adaptation> finalChoices = new ArrayList<Adaptation>();
    Long cost = Long.MAX_VALUE;
    Iterator<Adaptation> choiceIterator = choices.iterator();
    //calculate each cost, and compares it with the best so far, if the same, store it 
    while(choiceIterator.hasNext())
    {
      Adaptation choice = choiceIterator.next();
      Long choiceCost = choice.getEnergyCost() + choice.getTimeCost();
      if(choiceCost < cost)
      {
        finalChoices.clear();
        finalChoices.add(choice);
        cost = choiceCost;
      }
      if(choiceCost == cost)
      {
        finalChoices.add(choice);
      }
    }
    /*if only one best choice, return it 
    otherwise compare the cost of each tuple travelling up the tree to find the one with the least running cost.
    */
    if(finalChoices.size() == 1)
      return finalChoices.get(0);
    else
    {
      return compareRunTimeCosts(finalChoices);
    }
  }

  /**
   * goes though the left over choices and compares their run time costs, to find 
   * the cheapest adaptation in run time costs.
   * @param finalChoices
   * @return
   */
  private Adaptation compareRunTimeCosts(List<Adaptation> finalChoices)
  {
    Long runningCost = Long.MAX_VALUE;
    Adaptation finalChoice = null;
    Iterator<Adaptation> finalChoiceIterator = finalChoices.iterator();
    while(finalChoiceIterator.hasNext())
    {
      Adaptation currentFinalChoice = finalChoiceIterator.next();
      Long currentRunningCost = calculateRunningCost(currentFinalChoice);
      if(currentRunningCost < runningCost)
      {
        finalChoice = currentFinalChoice;
        runningCost = currentRunningCost;
      }
    } 
    return finalChoice;
  }

  /**
   * calculates running costs by tracking the cost of each tuple from source to sink
   * @param currentFinalChoice
   * @return
   */
  private Long calculateRunningCost(Adaptation currentFinalChoice)
  {
    RT routingTree = currentFinalChoice.getNewQep().getRT();
    
    // TODO Auto-generated method stub
    return Long.MAX_VALUE -1;
  }
  
}
