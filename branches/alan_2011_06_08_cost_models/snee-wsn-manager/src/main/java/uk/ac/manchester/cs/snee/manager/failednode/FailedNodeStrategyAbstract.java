package uk.ac.manchester.cs.snee.manager.failednode;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;

import com.rits.cloning.Cloner;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.iot.AgendaIOT;
import uk.ac.manchester.cs.snee.compiler.iot.IOT;
import uk.ac.manchester.cs.snee.compiler.iot.InstanceOperator;
import uk.ac.manchester.cs.snee.compiler.params.qos.QoSExpectations;
import uk.ac.manchester.cs.snee.compiler.queryplan.Agenda;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.PAF;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenScheduler;
import uk.ac.manchester.cs.snee.compiler.sn.when.WhenSchedulerException;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.StrategyAbstract;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetAcquireOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetExchangeOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperator;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetOperatorImpl;
import uk.ac.manchester.cs.snee.sncb.SNCBException;

public abstract class FailedNodeStrategyAbstract extends StrategyAbstract implements Serializable
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -1326297492822951325L;
  protected Cloner cloner;

  /**
   * bog standard constructor
   * @param manager
   */
  public FailedNodeStrategyAbstract(AutonomicManagerImpl manager, SourceMetadataAbstract _metadata)
  {
    this.manager = manager;
    this._metadata = _metadata;
    this.outputFolder = manager.getOutputFolder();
    cloner  = new Cloner();
    cloner.dontClone(Logger.class);
  }
  
  /**
   * creates a pinned PAF where operaotrs not on faield nodes are pinned to the sites.
   * @param agenda2
   * @param iot
   * @param failedNodes
   * @param depinnedNodes 
   * @throws SNEEException
   * @throws SchemaMetadataException
   * @throws SNEEConfigurationException
   * @throws OptimizationException 
   */
  protected PAF pinPhysicalOperators(IOT iot, ArrayList<String> failedNodes, 
                                   ArrayList<String> depinnedNodes) 
  throws SNEEException, 
         SchemaMetadataException, 
         SNEEConfigurationException, 
         OptimizationException
  {
    //get paf 
    Cloner cloner = new Cloner();
    cloner.dontClone(Logger.class);
    PAF paf = cloner.deepClone(iot.getPAF());
    //get iterator for IOT without exchanges
    Iterator<InstanceOperator> iotInstanceOperatorIterator = iot.treeIterator(TraversalOrder.PRE_ORDER, false);
    ArrayList<SensornetOperatorImpl> opsOnFailedNode = new ArrayList<SensornetOperatorImpl>();
    while(iotInstanceOperatorIterator.hasNext())
    {
      InstanceOperator instanceOperator = iotInstanceOperatorIterator.next();
      SensornetOperator physicalOperator = instanceOperator.getSensornetOperator();
      if(!physicalOperator.isAttributeSensitive())
      {
        SensornetOperatorImpl physicalOperatorImpl = (SensornetOperatorImpl) physicalOperator;
        if(!failedNodes.contains(instanceOperator.getSite().getID()) || !depinnedNodes.contains(instanceOperator.getSite().getID()))
        {
          ((SensornetOperatorImpl) paf.getOperatorTree().getNode(physicalOperatorImpl.getID())).setIsPinned(true);
          ((SensornetOperatorImpl) paf.getOperatorTree().getNode(physicalOperatorImpl.getID())).addSiteToPinnedList(instanceOperator.getSite().getID());
        }
        else
        {
          if(!(physicalOperator instanceof SensornetAcquireOperator) && 
             failedNodes.contains(instanceOperator.getSite().getID()))
            opsOnFailedNode.add(((SensornetOperatorImpl) paf.getOperatorTree().getNode(physicalOperatorImpl.getID())));
        }
      }
    }
    //remove total pinning on operators located on failed node
    Iterator<SensornetOperatorImpl> failedNodeOpIterator = opsOnFailedNode.iterator();
    while(failedNodeOpIterator.hasNext())
    {
      SensornetOperatorImpl physicalOperatorImpl = ((SensornetOperatorImpl) paf.getOperatorTree().getNode(failedNodeOpIterator.next().getID()));
      physicalOperatorImpl.setTotallyPinned(false);
    }
    
    paf = this.removeExchangesFromPAF(paf);
    paf.setID("PinnedPAF");
    return paf;
  }
  
  
  /**
   * helper method which removes Exchanges from a PAF
   * @param paf
   * @return
   * @throws OptimizationException
   */
  protected PAF removeExchangesFromPAF(PAF paf) 
  throws OptimizationException
  {
    //remove exchange operators (does not exist in a paf)
    Iterator<SensornetOperator> pafIterator = paf.operatorIterator(TraversalOrder.POST_ORDER);
    while(pafIterator.hasNext())
    {
      SensornetOperator physicalOperator = pafIterator.next();
      if(physicalOperator instanceof SensornetExchangeOperator)
      {
        paf.getOperatorTree().removeNode(physicalOperator);
      }
    }
    return paf;
  }
  
  /**
   * run when scheduling
   * @param newIOT
   * @param qos
   * @param id
   * @param costParameters
   * @return
   * @throws SNEEConfigurationException
   * @throws SNEEException
   * @throws SchemaMetadataException
   * @throws OptimizationException
   * @throws WhenSchedulerException
   * @throws MalformedURLException
   * @throws TypeMappingException
   * @throws MetadataException
   * @throws UnsupportedAttributeTypeException
   * @throws SourceMetadataException
   * @throws TopologyReaderException
   * @throws SNEEDataSourceException
   * @throws CostParametersException
   * @throws SNCBException
   * @throws SNEECompilerException 
   */
  protected AgendaIOT doSNWhenScheduling(IOT newIOT, QoSExpectations qos,
                                       String id, CostParameters costParameters)
  throws SNEEConfigurationException, SNEEException, 
  SchemaMetadataException, OptimizationException, 
  MalformedURLException, TypeMappingException, 
  MetadataException, UnsupportedAttributeTypeException, 
  SourceMetadataException, TopologyReaderException, 
  SNEEDataSourceException, CostParametersException, 
  SNCBException, SNEECompilerException 
  {
      boolean useNetworkController = SNEEProperties.getBoolSetting(
          SNEEPropertyNames.SNCB_INCLUDE_COMMAND_SERVER);
      boolean allowDiscontinuousSensing = SNEEProperties.getBoolSetting(
          SNEEPropertyNames.ALLOW_DISCONTINUOUS_SENSING);
      WhenScheduler whenSched = new WhenScheduler(allowDiscontinuousSensing, costParameters, useNetworkController);
      AgendaIOT agenda;
      try
      {
        agenda = whenSched.doWhenScheduling(newIOT, qos, currentQEP.getID(), currentQEP.getCostParameters());
      }
      catch (WhenSchedulerException e)
      {
        throw new SNEECompilerException(e);
      }  
      agenda.setID("new Agenda");
      return agenda;
  }


	/**
	 * run when scheduling
	 * @param newIOT
	 * @param qos
	 * @param id
	 * @param costParameters
	 * @return
	 * @throws SNEEConfigurationException
	 * @throws SNEEException
	 * @throws SchemaMetadataException
	 * @throws OptimizationException
	 * @throws WhenSchedulerException
	 * @throws MalformedURLException
	 * @throws TypeMappingException
	 * @throws MetadataException
	 * @throws UnsupportedAttributeTypeException
	 * @throws SourceMetadataException
	 * @throws TopologyReaderException
	 * @throws SNEEDataSourceException
	 * @throws CostParametersException
	 * @throws SNCBException
	 * @throws SNEECompilerException 
	 */
	protected Agenda doOldSNWhenScheduling(DAF daf, QoSExpectations qos,
	                                     String id, CostParameters costParameters)
	throws SNEEConfigurationException, SNEEException, 
	SchemaMetadataException, OptimizationException, 
	MalformedURLException, TypeMappingException, 
	MetadataException, UnsupportedAttributeTypeException, 
	SourceMetadataException, TopologyReaderException, 
	SNEEDataSourceException, CostParametersException, 
	SNCBException, SNEECompilerException 
	{
	  boolean useNetworkController = SNEEProperties.getBoolSetting(
	      SNEEPropertyNames.SNCB_INCLUDE_COMMAND_SERVER);
	  boolean allowDiscontinuousSensing = SNEEProperties.getBoolSetting(
	      SNEEPropertyNames.ALLOW_DISCONTINUOUS_SENSING);
	  WhenScheduler whenSched = new WhenScheduler(allowDiscontinuousSensing, costParameters, useNetworkController);
	  Agenda agenda;
	  try
	  {
	    agenda = whenSched.doWhenScheduling(daf, qos, currentQEP.getID());
	  }
	  catch (WhenSchedulerException e)
	  {
	    throw new SNEECompilerException(e);
	  }  
	  agenda.setID("new Agenda");
	  return agenda;
	}
}