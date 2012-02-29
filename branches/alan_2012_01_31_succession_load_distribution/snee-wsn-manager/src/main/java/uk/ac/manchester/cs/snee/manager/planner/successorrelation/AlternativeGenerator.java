package uk.ac.manchester.cs.snee.manager.planner.successorrelation;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.WhenSchedulerException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceWhereSchedular;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenScheduler;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.AutonomicManagerComponent;
import uk.ac.manchester.cs.snee.manager.failednode.alternativerouter.CandiateRouter;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;

public class AlternativeGenerator extends AutonomicManagerComponent
{

  private static final long serialVersionUID = -2274933349883496847L;
  private SensorNetworkQueryPlan qep;
  private Topology network;
  private File outputFolder;
  private SourceMetadataAbstract _metadata;
  private String sep = System.getProperty("file.separator");
  
  /**
   * constructor
   * @param qep
   * @param topology 
   * @param _metadata 
   */
  public AlternativeGenerator(SensorNetworkQueryPlan qep, Topology topology, 
                              File outputFolder, SourceMetadataAbstract _metadata, 
                              AutonomicManagerImpl manager)
  {
	  this.qep = qep;
	  this.manager = manager;
	  this.network = topology;
	  this.outputFolder = outputFolder;
	  this._metadata = _metadata;
	  
  }
  
  /**
   * generates a set of alternative qeps by using different decisions from the snee stack. 
   * @return
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   * @throws OptimizationException 
   * @throws SchemaMetadataException 
   * @throws SNEEException 
   * @throws WhenSchedulerException 
   * @throws TypeMappingException 
   */
  public ArrayList<SensorNetworkQueryPlan> generateAlternatives() 
  throws NumberFormatException, SNEEConfigurationException, 
  SNEEException, SchemaMetadataException, OptimizationException, 
  WhenSchedulerException, TypeMappingException
  {
    ArrayList<SensorNetworkQueryPlan> qeps = new ArrayList<SensorNetworkQueryPlan>();
    CandiateRouter metaRouter = new CandiateRouter(network, outputFolder, this.qep.getDAF().getPAF(), 
                                                   this._metadata);
	  ArrayList<RT> candidateRoutes = metaRouter.generateAlternativeRoutingTrees(this.qep.getQueryName());
	  
	  
	  Iterator<RT> routeIterator = candidateRoutes.iterator();
    int routeCounter = 1;
    while(routeIterator.hasNext())
    {
      File planOutputFolder =  new File(this.outputFolder.toString() + sep + "plan" + routeCounter);
      planOutputFolder.mkdir();
      RT routingTree = routeIterator.next();
      //do where scheduling
      InstanceWhereSchedular whereScheduling = new InstanceWhereSchedular(this.qep.getDAF().getPAF(),
                                                                          routingTree, 
                                                                          this.qep.getCostParameters(),
                                                                          planOutputFolder.toString());
      IOT iot = whereScheduling.getIOT();
      DAF daf = whereScheduling.getDAF();
      
      //do when scheduling
      boolean useNetworkController = SNEEProperties.getBoolSetting(SNEEPropertyNames.SNCB_INCLUDE_COMMAND_SERVER);
      boolean allowDiscontinuousSensing = SNEEProperties.getBoolSetting(SNEEPropertyNames.ALLOW_DISCONTINUOUS_SENSING);
      WhenScheduler whenScheduling = new WhenScheduler(allowDiscontinuousSensing, this.qep.getCostParameters(), useNetworkController);
      Agenda agenda = whenScheduling.doWhenScheduling(daf, this.qep.getQos(), this.qep.getQueryName());
      AgendaIOT agendaIOT = whenScheduling.doWhenScheduling(iot, this.qep.getQos(), this.qep.getQueryName(), this.qep.getCostParameters());
      qeps.add(new SensorNetworkQueryPlan(this.qep.getDLAF(), routingTree, daf, iot, agendaIOT, agenda, this.qep.getQueryName() + ":ALT" + routeCounter ));
      routeCounter++;
    }
    return qeps;
  }
} 
