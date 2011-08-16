package uk.ac.manchester.cs.snee.manager.planner;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.Adaptation;
import uk.ac.manchester.cs.snee.manager.AdaptationUtils;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
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

  public Adaptation assessChoices(List<Adaptation> choices) 
  throws IOException, OptimizationException, 
  SchemaMetadataException, TypeMappingException
  {
    AssessmentFolder = new File(outputFolder.toString() + sep + "assessment");
    AssessmentFolder.mkdir();
    
    System.out.println("Starting assessment of choices");
    Iterator<Adaptation> choiceIterator = choices.iterator();
    while(choiceIterator.hasNext())
    {
      Adaptation adapt = choiceIterator.next();
      adapt.setTimeCost(this.timeCost(adapt));
      adapt.setEnergyCost(this.energyCost(adapt));
      adapt.setRuntimeCost(this.runTimeCost(adapt));
      adapt.setLifetimeEstimate(this.estimatedLifetime(adapt));
      
    }
    new AdaptationUtils(choices, _metadataManager.getCostParameters()).FileOutput(AssessmentFolder);
    
    return this.locateBestAdaptation(choices);
  }
  
  
  
  /**
   * method to determine the estimated lifetime of the new QEP
   * @param adapt
   * @return
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  private Double estimatedLifetime(Adaptation adapt) 
  throws OptimizationException, SchemaMetadataException, 
  TypeMappingException
  {
    double shortestLifetime = Double.MAX_VALUE; //s
    
    Iterator<Site> siteIter = adapt.getNewQep().getIOT().getRT().siteIterator(
        TraversalOrder.POST_ORDER);
    while (siteIter.hasNext()) {
      Site site = siteIter.next();
      if (site!=adapt.getNewQep().getIOT().getRT().getRoot()) {
        
        double siteEnergySupply = site.getEnergyStock()/1000.0; // mJ to J 
        double siteEnergyCons = adapt.getNewQep().getAgendaIOT().getSiteEnergyConsumption(site); // J
        double agendaLength = Agenda.bmsToMs(adapt.getNewQep().getAgendaIOT().getLength_bms(false))/1000.0; // ms to s
        double energyConsumptionRate = siteEnergyCons/agendaLength; // J/s
        double siteLifetime = siteEnergySupply / energyConsumptionRate; //s
      
        shortestLifetime = Math.min((double)shortestLifetime, siteLifetime);
      }
    }// TODO Auto-generated method stub
    return shortestLifetime;
  }

  /**
   * method to determine cost of running new QEP for an agenda execution cycle
   * @param adapt
   * @return
   */
  private Long runTimeCost(Adaptation adapt)
  {
    // TODO Auto-generated method stub
    return (long) 0;
  }

  /**
   * Method which determines the energy cost of making the adaptation.
   * @param adapt
   * @return returns the cost in energy units.
   */
  private Long energyCost(Adaptation adapt)
  {
    // TODO Auto-generated method stub
    return (long) 0;
  }
  
  /**
   * Method which determines the time cost of an adaptation
   * @param adapt
   * @return
   */
  private Long timeCost(Adaptation adapt)
  {
    // TODO Auto-generated method stub
    return (long) 0;
  }
  
  /**
   * goes though the choices list locating the one with the biggest lifetime
   * @param choices
   * @return
   */
  private Adaptation locateBestAdaptation(List<Adaptation> choices)
  {
    Adaptation finalChoice = null;
    Double cost = Double.MAX_VALUE;
    Iterator<Adaptation> choiceIterator = choices.iterator();
    //calculate each cost, and compares it with the best so far, if the same, store it 
    while(choiceIterator.hasNext())
    {
      Adaptation choice = choiceIterator.next();
      Double choiceCost = choice.getLifetimeEstimate();
      if(choiceCost < cost)
      {
        finalChoice = choice;
        cost = choiceCost;
      }
    }
    return finalChoice;
  }
}
