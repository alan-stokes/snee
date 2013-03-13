package uk.ac.manchester.cs.snee.manager.planner.successorrelation;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import com.rits.cloning.Cloner;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.AgendaException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.WhenSchedulerException;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceOperator;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.AutonomicManagerComponent;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.completerecompilationstrategy.CompleteReCompilationStrategy;
import uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.energy.SiteEnergyModel;
import uk.ac.manchester.cs.snee.manager.planner.successorrelation.successor.Successor;
import uk.ac.manchester.cs.snee.manager.planner.successorrelation.successor.SuccessorPath;
import uk.ac.manchester.cs.snee.manager.planner.successorrelation.tabu.TabuSearch;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAcquireOperator;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

public class SuccessorRelationManager extends AutonomicManagerComponent
{
  private static final long serialVersionUID = 891621010871659913L;
  private File successorFolder = null;
  private HashMap<String, RunTimeSite> runningSites;
  private MetadataManager _metadataManager;
  private Topology deployment;
  private FailedNodeData failedNode = null;
  private CompleteReCompilationStrategy globalAdaptationStrategy = null;
  private Cloner cloner = new Cloner();
  public SuccessorRelationManager(File plannerFolder, HashMap<String, RunTimeSite> runningSites,
                           MetadataManager _metadataManager, SourceMetadataAbstract _metadata, AutonomicManagerImpl manager)
  {
    this.manager = manager;
    cloner.dontClone(Logger.class);
    this._metadata = _metadata;
    deployment = this.manager.getWsnTopology();
    successorFolder = new File(plannerFolder.toString() + sep + "successorRelation");
    if(successorFolder.exists())
    {
      manager.deleteFileContents(successorFolder);
      successorFolder.mkdir();
    }
    else
    {
      successorFolder.mkdir();
    }
    this.runningSites = runningSites;
    this._metadataManager = _metadataManager;
  }
  
  public SuccessorPath executeSuccessorRelation(SensorNetworkQueryPlan initialPoint)
  {
    try
    {
      TabuSearch search = null;
      //set up TABU folder
      File TABUFolder = new File(successorFolder.toString() + sep + "TABU");
      if(TABUFolder.exists())
      {
        manager.deleteFileContents(TABUFolder);
        TABUFolder.mkdir();
      }
      else
      {
        TABUFolder.mkdir();
      }
      //search though space
      search = new TabuSearch(manager.getWsnTopology(), runningSites, _metadata, _metadataManager, TABUFolder);
      SuccessorPath bestSuccessorRelation = search.findSuccessorsPath(initialPoint);
      new SuccessorRelationManagerUtils(this.manager, successorFolder).writeSuccessorToFile(bestSuccessorRelation.getSuccessorList(), "finalSolution");
      
      testAdaptiveLifetime(bestSuccessorRelation);
      
    // writeSuccessorPathToFile(bestSuccessorRelation);
    // SuccessorPath bestSuccessorRelation = readInSuccessor();
      //new PlannerUtils(successorFolder, this.manager).writeSuccessorToFile(bestSuccessorRelation.getSuccessorList(), "finalSolution");
     
      //added code to see how well tuned the plan is without recomputing
      //  BufferedWriter out = new BufferedWriter(new FileWriter(new File(successorFolder.toString() + sep + "records")));
      //  SuccessorPath twiddleBestSuccessorRelation = TimeTwiddler.adjustTimesTest(bestSuccessorRelation, runningSites, false, search, out);      
     //  new PlannerUtils(successorFolder, this.manager).writeSuccessorToFile(twiddleBestSuccessorRelation.getSuccessorList(), "finalTwiddleSolution");
        //out.close();
        //added code to see how well tuned successor is by adjusting and then running entire system
   //   SuccessorPath twiddleBestSuccessorRelation = TimeTwiddler.adjustTimesTest(bestSuccessorRelation, runningSites, true, search);      
     // new PlannerUtils(successorFolder, this.manager).writeSuccessorToFile(twiddleBestSuccessorRelation.getSuccessorList(), "finalTwiddleSolutionWithRecompute");
      
      return bestSuccessorRelation;
    }
    catch(Exception e)
    {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * method that evaluates the successor relation given a collection of random node failures during its expected lifetime.
   *Compares both global and successor relation. (needs to only work on confulance nodes)
   * @param bestSuccessorRelation
   * @throws SNEEConfigurationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws SNEECompilerException 
   * @throws SNCBException 
   * @throws CostParametersException 
   * @throws SNEEDataSourceException 
   * @throws MetadataException 
   * @throws TopologyReaderException 
   * @throws SNEEException 
   * @throws AgendaException 
   * @throws SourceMetadataException 
   * @throws UnsupportedAttributeTypeException 
   * @throws NumberFormatException 
   * @throws WhenSchedulerException 
   * @throws CodeGenerationException 
   * @throws IOException 
   */
  private void testAdaptiveLifetime(SuccessorPath bestSuccessorRelation) 
  throws OptimizationException, SchemaMetadataException, 
  TypeMappingException, SNEEConfigurationException, NumberFormatException, 
  UnsupportedAttributeTypeException, SourceMetadataException, 
  AgendaException, SNEEException, TopologyReaderException, MetadataException, 
  SNEEDataSourceException, CostParametersException, SNCBException, SNEECompilerException,
  IOException, CodeGenerationException, WhenSchedulerException
  {
    this.globalAdaptationStrategy = 
      new CompleteReCompilationStrategy(this.manager, this._metadata, this._metadataManager);
    int overallSuccessorPathLifetime = bestSuccessorRelation.overallSuccessorPathLifetime();
    int noNodefails = 1;
    boolean globalFailed = false;
    boolean successorFailed = false;
    ArrayList<Integer> globalLifetimes = new ArrayList<Integer>();
    ArrayList<Integer> successorLifetimes = new ArrayList<Integer>();
    for(int index =0; index <=8; index++)
    {
      globalLifetimes.add(null);
      successorLifetimes.add(null);
    }
    while(noNodefails <= 8 && !globalFailed)
    {
      //collects the global QEP;
      SensorNetworkQueryPlan golbalQEP = bestSuccessorRelation.getSuccessorList().get(0).getQep();
      golbalQEP = cloner.deepClone(golbalQEP);
      //determines the time period for the unpredictable node failure
      double timeOfNodeFailure = overallSuccessorPathLifetime / (noNodefails + 1);
      ArrayList<String> globalFailedNodes = new ArrayList<String>();
      HashMap<String, RunTimeSite> GlobalRunningSites = manager.getCopyOfRunningSites();
      //do globals node failure lifetime
      boolean successful = doGlobalAdaptations(timeOfNodeFailure, globalFailedNodes, GlobalRunningSites, 
                                               golbalQEP, noNodefails, globalLifetimes);
      globalFailed = !successful;
      if(!globalFailed)
        noNodefails++;
    }
    noNodefails = 1;
    while(noNodefails <= 8 && !successorFailed)
    {
      SuccessorPath successorRelation = cloner.deepClone(bestSuccessorRelation);
      //determines the time period for the unpredictable node failure
      double timeOfNodeFailure = overallSuccessorPathLifetime / (noNodefails + 1);
      ArrayList<String> globalFailedNodes = new ArrayList<String>();
      HashMap<String, RunTimeSite> SuccessorRunningSites = manager.getCopyOfRunningSites();
      //do successor node failure lifetime
      boolean successful = doSucessorAdaptations(timeOfNodeFailure, globalFailedNodes, SuccessorRunningSites, 
                                                 successorRelation, noNodefails, successorLifetimes); 
      successorFailed = !successful;
      if(!successorFailed)
        noNodefails++;
    }
    outputData(globalLifetimes, successorLifetimes,bestSuccessorRelation);
    
  }
  
  
  
  private void outputData(ArrayList<Integer> globalLifetimes,
                          ArrayList<Integer> successorLifetimes, SuccessorPath bestSuccessorRelation                          ) 
  throws IOException
  {
     BufferedWriter out = new BufferedWriter(new FileWriter(new File(successorFolder.toString() + sep + "results")));
     for(int index = 0; index<= 8; index++)
     {
       Integer successorLifetime = successorLifetimes.get(index);
       Integer globalLifetime =  globalLifetimes.get(index);
       if(successorLifetime == null)
         successorLifetime = 0;
       if(globalLifetime == null)
         globalLifetime = 0;
       globalLifetime = new Double(Math.floor(globalLifetime)).intValue();
       successorLifetime = new Double(Math.floor(successorLifetime)).intValue();
       
       out.write(index+1 + " " + globalLifetime + " " + successorLifetime + " \n");
     }
     out.flush();
     out.close();
    
  }

  /**
   * executes the successor relation for unpredictable node failure.
   * @param timeOfNodeFailure
   * @param globalFailedNodes
   * @param successorRunningSites
   * @param bestSuccessorRelation
   * @param noNodefails
   * @param successorLifetimes 
   * @param currentAgendaCycle
   * @return
   * @throws OptimizationException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws SNEEConfigurationException
   * @throws NumberFormatException
   * @throws UnsupportedAttributeTypeException
   * @throws SourceMetadataException
   * @throws AgendaException
   * @throws SNEEException
   * @throws TopologyReaderException
   * @throws MetadataException
   * @throws SNEEDataSourceException
   * @throws CostParametersException
   * @throws SNCBException
   * @throws SNEECompilerException
   * @throws IOException
   * @throws CodeGenerationException
   * @throws WhenSchedulerException
   */
  private boolean doSucessorAdaptations(double timeOfNodeFailure, ArrayList<String> globalFailedNodes,
                                     HashMap<String, RunTimeSite> successorRunningSites,
                                     SuccessorPath bestSuccessorRelation, int noNodefails,
                                     ArrayList<Integer> successorLifetimes)
  throws OptimizationException, SchemaMetadataException, TypeMappingException, 
         SNEEConfigurationException, NumberFormatException, UnsupportedAttributeTypeException, 
         SourceMetadataException, AgendaException, 
         SNEEException, TopologyReaderException, MetadataException, SNEEDataSourceException, 
         CostParametersException, SNCBException, SNEECompilerException,
         IOException, CodeGenerationException, WhenSchedulerException
  {
    int currentNodeFailure = 1;
    int currentAgendaCycle = 0;
    Topology currentRunDeployment = cloner.deepClone(this.deployment);
    boolean failed = false;
    while(currentNodeFailure <= noNodefails && !failed)
    {
      System.out.println("doing successor relation for node failure " + currentNodeFailure + " out of "+ noNodefails);
      timeOfNodeFailure = (timeOfNodeFailure * currentNodeFailure) - currentAgendaCycle;
      
      SuccessorLifetimeEnum result = updateSitesEnergyLevelsForSuccessor(timeOfNodeFailure,
                                                                      globalFailedNodes, 
                                                                      bestSuccessorRelation,
                                                                      currentAgendaCycle,
                                                                      successorRunningSites);
      if(result.equals(SuccessorLifetimeEnum.CLEARED))
      {
        ArrayList<Successor> activeSuccessors = 
          findCorrectSuccessorsForTimePeriod(bestSuccessorRelation, timeOfNodeFailure, currentAgendaCycle);
        String failedNode = locateFailedNode(activeSuccessors.get(activeSuccessors.size() -1).getQep());
        ArrayList<String> failedNodes = new ArrayList<String>();
        failedNodes.add(failedNode);
        List<Adaptation> adaptations = 
          this.globalAdaptationStrategy.adapt(failedNodes, currentRunDeployment, 
                                              activeSuccessors.get(activeSuccessors.size() -1).getQep());
        if(adaptations.size() == 0)
          failed = true;
        else
        {
          SensorNetworkQueryPlan seed = adaptations.get(0).getNewQep();
          TabuSearch search=  new TabuSearch(manager.getWsnTopology(), runningSites, _metadata, 
                                             _metadataManager, successorFolder, failedNodes);
          bestSuccessorRelation = search.findSuccessorsPath(seed);
        }
        currentNodeFailure++;
        currentAgendaCycle += timeOfNodeFailure;
      }
      if(result.equals(SuccessorLifetimeEnum.NODEFAILEDBYENERGY))
      {
        String failedNode = this.failedNode.getNode().getID();
        ArrayList<String> failedNodes = new ArrayList<String>();
        failedNodes.add(failedNode);
        List<Adaptation> adaptations = 
          this.globalAdaptationStrategy.adapt(failedNodes, currentRunDeployment, 
                                              locateCorrectSuccessor(bestSuccessorRelation).getQep());
        if(adaptations.size() == 0)
          failed = true;
        else
        {
          SensorNetworkQueryPlan seed = adaptations.get(0).getNewQep();
          seed.getRT().setNetwork(cloner.deepClone(deployment));
          TabuSearch search=  new TabuSearch(cloner.deepClone(deployment), runningSites, _metadata, _metadataManager, successorFolder);
          bestSuccessorRelation = search.findSuccessorsPath(seed);
        }
        currentAgendaCycle += this.failedNode.getLifetime();
      }
    }
    successorLifetimes.add(noNodefails -1, bestSuccessorRelation.overallSuccessorPathLifetime());
    return !failed;
  }

  /**
   * give a time period for a node fialure, lcoates which successor it would be in
   * @param bestSuccessorRelation
   * @param lifetime
   * @return
   */
  private Successor locateCorrectSuccessor(SuccessorPath bestSuccessorRelation)
  {
    Iterator<Successor> successorIterator = bestSuccessorRelation.getSuccessorList().iterator();
    while(successorIterator.hasNext())
    {
      Successor curent = successorIterator.next();
      if(curent.toString().equals(this.failedNode.getSuccessorID()))
        return curent;
    }
    return null;
  }

  /**
   * executes the globla adaptations
   * @param timeOfNodeFailure
   * @param globalFailedNodes
   * @param globalRunningSites
   * @param golbalQEP
   * @param noNodefails
   * @param globalLifetimes 
   * @return
   * @throws OptimizationException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws SNEEConfigurationException
   * @throws NumberFormatException
   * @throws MalformedURLException
   * @throws UnsupportedAttributeTypeException
   * @throws SourceMetadataException
   * @throws AgendaException
   * @throws SNEEException
   * @throws TopologyReaderException
   * @throws MetadataException
   * @throws SNEEDataSourceException
   * @throws CostParametersException
   * @throws SNCBException
   * @throws SNEECompilerException
   */
  private boolean doGlobalAdaptations(double timeOfNodeFailure, ArrayList<String> globalFailedNodes,
                                   HashMap<String, RunTimeSite> globalRunningSites,
                                   SensorNetworkQueryPlan golbalQEP, int noNodefails,
                                   ArrayList<Integer> globalLifetimes) 
  throws OptimizationException, SchemaMetadataException, 
         TypeMappingException, SNEEConfigurationException,
         NumberFormatException, MalformedURLException, 
         UnsupportedAttributeTypeException, SourceMetadataException, 
         AgendaException, SNEEException, TopologyReaderException, 
         MetadataException, SNEEDataSourceException, CostParametersException, 
         SNCBException, SNEECompilerException
  {
    int currentNodeFailure = 1;
    Topology currentRunDeployment = cloner.deepClone(this.deployment);
    boolean failed = false;
    int currentAgendaCycle = 0;
    
    while(currentNodeFailure <= noNodefails && !failed)
    {
      timeOfNodeFailure = (timeOfNodeFailure * currentNodeFailure) - currentAgendaCycle;
      SuccessorLifetimeEnum result = updateSitesEnergyLevelsForGlobal(timeOfNodeFailure,
                                                                      globalFailedNodes, 
                                                                      globalRunningSites, golbalQEP);
      if(result.equals(SuccessorLifetimeEnum.CLEARED))
      {
        String failedNode = locateFailedNode(golbalQEP);
        ArrayList<String> failedNodes = new ArrayList<String>();
        failedNodes.add(failedNode);
        List<Adaptation> adaptations = this.globalAdaptationStrategy.adapt(failedNodes, currentRunDeployment, golbalQEP);
        if(adaptations.size() == 0)
          failed = true;
        else
          golbalQEP = adaptations.get(0).getNewQep();
        currentNodeFailure++;
        currentAgendaCycle += timeOfNodeFailure;
      }
      if(result.equals(SuccessorLifetimeEnum.NODEFAILEDBYENERGY))
      {
        String failedNode = this.failedNode.getNode().getID();
        ArrayList<String> failedNodes = new ArrayList<String>();
        failedNodes.add(failedNode);
        List<Adaptation> adaptations = this.globalAdaptationStrategy.adapt(failedNodes, currentRunDeployment, golbalQEP);
        if(adaptations.size() == 0)
          failed = true;
        else
          golbalQEP = adaptations.get(0).getNewQep();
        currentAgendaCycle += this.failedNode.getLifetime();
      }
    }
    Double lifetime = calculateLifetime(golbalQEP, globalRunningSites, globalFailedNodes, currentRunDeployment);
    globalLifetimes.set(noNodefails -1, new Double(new Double(lifetime + currentAgendaCycle) / 
                       (Agenda.bmsToMs(golbalQEP.getAgendaIOT().getLength_bms(false)) / 1000)).intValue());
    return !failed;
  }

  private Double calculateLifetime(SensorNetworkQueryPlan golbalQEP,
                                   HashMap<String, RunTimeSite> globalRunningSites,
                                   ArrayList<String> globalFailedNodes, Topology currentRunDeployment)
  throws OptimizationException, SchemaMetadataException, TypeMappingException, SNEEConfigurationException,
         NumberFormatException, MalformedURLException, UnsupportedAttributeTypeException, 
         SourceMetadataException, AgendaException, SNEEException, TopologyReaderException, 
         MetadataException, SNEEDataSourceException, CostParametersException, SNCBException, 
         SNEECompilerException
  {
    boolean failed = false;
    double lifetime = 0;
    do
    {
      FailedNodeData failedNode = firstNodeToFailFromEnergyDepletion(golbalQEP, globalRunningSites);
      lifetime += failedNode.getLifetime();
      this.updateSitesEnergyLevelsForGlobal(failedNode.getLifetime(), globalFailedNodes, globalRunningSites, golbalQEP);
      ArrayList<String> failedNodes = new ArrayList<String>();
      failedNodes.add(failedNode.getNode().getID());
      failedNodes.addAll(globalFailedNodes);
      List<Adaptation> adaptations = this.globalAdaptationStrategy.adapt(failedNodes, currentRunDeployment, golbalQEP);
      if(adaptations.size() == 0)
        failed = true;
      else
      {
        golbalQEP = adaptations.get(0).getNewQep();
        this.updateRunningSites(golbalQEP, globalRunningSites);
      }
    }while(!failed);
    return lifetime;
  }

  /**
   * takes a QEP and randomly selects a non-acquire node for failure
   * @param QEP
   * @return
   */
  private String locateFailedNode(SensorNetworkQueryPlan QEP)
  {
    Random random = new Random(0);
    ArrayList<String> collection = new ArrayList<String>();
    Iterator<Integer> rtIterator = QEP.getRT().getSiteIDs().iterator();
    while(rtIterator.hasNext())
    {
      Integer siteID = rtIterator.next();
      ArrayList<InstanceOperator> opsOnSite = 
        QEP.getIOT().getOpInstances(QEP.getIOT().getSiteFromID(siteID.toString()));
      Iterator<InstanceOperator> operatorIterator = opsOnSite.iterator();
      boolean acquire = false;
      while(operatorIterator.hasNext())
      {
        InstanceOperator operator = operatorIterator.next();
        if(operator.getSensornetOperator() instanceof SensornetAcquireOperator)
          acquire = true;
      }
      if(!acquire && !QEP.getRT().getRoot().getID().equals(siteID.toString()))
        collection.add(siteID.toString());
    }
    return collection.get(random.nextInt(collection.size() -1));
  }

  /**
   * updates running sites for the period of QEP running for the global version
   * @param shortestLifetime
   * @param globalFailedNodes
   * @param currentAgendaCycle 
   * @throws SNEEConfigurationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  protected SuccessorLifetimeEnum updateSitesEnergyLevelsForGlobal(Double shortestLifetime, 
                                                  ArrayList<String> globalFailedNodes,
                                                  HashMap<String, RunTimeSite> GlobalRunningSites,
                                                  SensorNetworkQueryPlan golbalQEP)
  throws OptimizationException, SchemaMetadataException, TypeMappingException, 
  SNEEConfigurationException
  {
    FailedNodeData firstNodeToFail = 
      firstNodeToFailFromEnergyDepletion(golbalQEP, GlobalRunningSites);
    if(firstNodeToFail.getLifetime() <= shortestLifetime)
    {
      this.failedNode = firstNodeToFail;
      updateRunningSites(golbalQEP, GlobalRunningSites);
      updateSitesEnergyLevels(firstNodeToFail.getLifetime(), globalFailedNodes, GlobalRunningSites);
      return SuccessorLifetimeEnum.NODEFAILEDBYENERGY;
    }
    else
    {
      updateRunningSites(golbalQEP, GlobalRunningSites);
      updateSitesEnergyLevels(shortestLifetime, globalFailedNodes, GlobalRunningSites);
    }
    return SuccessorLifetimeEnum.CLEARED;
  }
  
  /**
   * updates running sites for the period of QEP running for the succesosr version
   * @param shortestLifetime
   * @param globalFailedNodes
   * @throws SNEEConfigurationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  protected SuccessorLifetimeEnum updateSitesEnergyLevelsForSuccessor(Double shortestLifetime, 
                                                  ArrayList<String> globalFailedNodes,
                                                  SuccessorPath bestSuccessorRelation,
                                                  int currentAgendaCycleCount,
                                                  HashMap<String, RunTimeSite> SuccessorRunningSites) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException,
  SNEEConfigurationException
  {
    ArrayList<Successor> successorList= 
      findCorrectSuccessorsForTimePeriod(bestSuccessorRelation, shortestLifetime, currentAgendaCycleCount);
    if(successorList.size() == 1)
    {
      updateRunningSites(successorList.get(0).getQep(), SuccessorRunningSites);
      FailedNodeData firstNodeToFail = 
        firstNodeToFailFromEnergyDepletion(successorList.get(0).getQep(), SuccessorRunningSites);
      if(firstNodeToFail.getLifetime() <= shortestLifetime)
      {
        this.failedNode = firstNodeToFail;
        updateSitesEnergyLevels(firstNodeToFail.getLifetime(), globalFailedNodes, SuccessorRunningSites);
        return SuccessorLifetimeEnum.NODEFAILEDBYENERGY;
      }
      else
      {
        updateSitesEnergyLevels(firstNodeToFail.getLifetime(), globalFailedNodes, SuccessorRunningSites);
      }
    }
    else
    {
      Iterator<Successor> successors = successorList.iterator();
      while(successors.hasNext())
      {
        Successor currentSuccessor = successors.next();
        updateRunningSites(currentSuccessor.getQep(), SuccessorRunningSites);
        Integer timeInThisSuccessor = currentSuccessor.getAgendaCount();
        Double timeActaullyRunningInSuccessor = (double) (timeInThisSuccessor - 
          (currentAgendaCycleCount + currentSuccessor.getPreviousAgendaCount()));
        FailedNodeData firstNodeToFail = 
          firstNodeToFailFromEnergyDepletion(currentSuccessor.getQep(), SuccessorRunningSites);
        firstNodeToFail.setSuccessorID(currentSuccessor.toString());
        if(firstNodeToFail.getLifetime() <= timeActaullyRunningInSuccessor)
        {
          updateSitesEnergyLevels(firstNodeToFail.getLifetime(), globalFailedNodes,
                                  SuccessorRunningSites);
          this.failedNode = firstNodeToFail;
          return SuccessorLifetimeEnum.NODEFAILEDBYENERGY;
        }
        else
        {
          updateSitesEnergyLevels(timeActaullyRunningInSuccessor, globalFailedNodes,
              SuccessorRunningSites);
        }
      }
    }
    return SuccessorLifetimeEnum.CLEARED;
  }

  /**
   * removes a set of energy frome ach ndoe in the runningSites.
   * @param lifetime
   * @param globalFailedNodes
   * @param RunningSites
   */
  private void updateSitesEnergyLevels(Double lifetime,
                                       ArrayList<String> globalFailedNodes,
                                       HashMap<String, RunTimeSite> runningSites)
  {
    Iterator<Node> siteIter = this.deployment.siteIterator();
    lifetime = Math.floor(lifetime);
    while (siteIter.hasNext()) 
    {
      Node site = siteIter.next();
      if(!globalFailedNodes.contains(site.getID()))
      {
        RunTimeSite rSite = runningSites.get(site.getID());
        rSite.removeDefinedCost(rSite.getQepExecutionCost() * lifetime);
      }
    }
  }

  /**
   * determines which node is to fail first, and when
   * @param successorRunningSites
   * @return
   */
  private FailedNodeData firstNodeToFailFromEnergyDepletion(SensorNetworkQueryPlan QEP,
                                         HashMap<String, RunTimeSite> successorRunningSites)
  {
    double shortestLifetime = Double.MAX_VALUE; //s
    Iterator<Node> siteIter = this.deployment.getNodes().iterator();     
    FailedNodeData data = null;
    while (siteIter.hasNext()) 
    {
      Site site = (Site) siteIter.next();
      RunTimeSite rSite = runningSites.get(site.getID());
      double currentEnergySupply = rSite.getCurrentEnergy();
      double siteEnergyCons =  runningSites.get(site.getID()).getQepExecutionCost();
      double agendaLength = Agenda.bmsToMs(QEP.getAgendaIOT().getLength_bms(false))/new Double(1000); // ms to s
      double siteLifetime = (currentEnergySupply / siteEnergyCons) * agendaLength;
      //uncomment out sections to not take the root site into account
      if (!site.getID().equals(QEP.getIOT().getRT().getRoot().getID())) 
      { 
        if(shortestLifetime > siteLifetime)
        {
          if(!site.isDeadInSimulation())
          {
            shortestLifetime = siteLifetime;
            data = new FailedNodeData(site, shortestLifetime);
          }
        }
      }
    }
    return data;
  }

  /**
   * updates the running sites with the energy cost of running the qep's code.
   * @param qep
   * @throws SNEEConfigurationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  private void updateRunningSites(SensorNetworkQueryPlan qep,  
                                  HashMap<String, RunTimeSite> runningSites)
  throws OptimizationException, SchemaMetadataException, TypeMappingException,
  SNEEConfigurationException
  {
    qep.getAgendaIOT().getIOT().getRT().setNetwork(getWsnTopology());
    SiteEnergyModel siteModel = new SiteEnergyModel(qep.getAgendaIOT());
    Iterator<Node> siteIter = this.deployment.siteIterator();
    while (siteIter.hasNext()) 
    {
      Site site = (Site) siteIter.next();
      double siteEnergyCons = siteModel.getSiteEnergyConsumption(site); // J
      runningSites.get(site.getID()).setQepExecutionCost(siteEnergyCons);
    }
  }
  
  

  /**
   * locates the successors that should be considered in the energy depletion calculation
   * @param bestSuccessorRelation
   * @param shortestLifetime
   * @param currentAgendaCycleCount
   */
  private ArrayList<Successor> findCorrectSuccessorsForTimePeriod(SuccessorPath bestSuccessorRelation, 
                                                  Double shortestLifetime,
                                                  int currentAgendaCycleCount)
  {
    ArrayList<Successor> scopedSuccessors = new ArrayList<Successor>();
    Iterator<Successor> successors = bestSuccessorRelation.getSuccessorList().iterator();
    while(successors.hasNext())
    {
      Successor currentSuccessor = successors.next();
      if(currentSuccessor.getPreviousAgendaCount() <  shortestLifetime)
        scopedSuccessors.add(currentSuccessor);
    }
    return scopedSuccessors;
  }
  
  

  /**
   * code to read in a successor to bypass running search if best path is already known
   * @return
   */
  /*
  private SuccessorPath readInSuccessor()
  {
    try
    {
      //use buffering
      File f = new File("s/s" );
      System.out.println(f.getAbsolutePath());
      if(f.exists())
      {
      InputStream file = new FileInputStream("/local/SNEEUnreliableChannels/SNEE/clients/successorClient/successorFile");
      InputStream buffer = new BufferedInputStream( file );
      ObjectInput input = new ObjectInputStream ( buffer );
      //deserialize the List
      int b = input.available();
      Object x = input.readObject();
      SuccessorPath recoveredsuccessor = (SuccessorPath)x;
      input.close();
      return recoveredsuccessor;
      }
      else
      {
        System.out.println("file does not exist, at file path " + f.getAbsolutePath());
        return null;
      }
    }
    catch(Exception e)
    {
      e.printStackTrace();
      System.out.println("read in successor failed" + e.getMessage());
      return null;
    }
  }

  private void writeSuccessorPathToFile(SuccessorPath bestSuccessorRelation)
  {
    try
    {
      OutputStream file = new FileOutputStream( successorFolder.toString() + sep + "successorFile" );
      OutputStream buffer = new BufferedOutputStream( file );
      ObjectOutput output = new ObjectOutputStream( buffer );
      output.writeObject(bestSuccessorRelation);
      output.close();
    }
    catch(Exception e)
    {
      System.out.println("cannot write successorpath to file");
    }
  }*/
}
