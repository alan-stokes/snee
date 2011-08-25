package uk.ac.manchester.cs.snee.manager;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Map;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.ResultStore;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;
import uk.ac.manchester.cs.snee.sncb.SNCBException;
import uk.ac.manchester.cs.snee.sncb.SNCBSerialPortReceiver;

public interface AutonomicManager
{

  public abstract void runFailedNodeFramework(ArrayList<String> failedNodes)
      throws SNEEConfigurationException, OptimizationException,
      SchemaMetadataException, TypeMappingException, AgendaException,
      SNEEException, MalformedURLException,
      MetadataException, UnsupportedAttributeTypeException,
      SourceMetadataException, TopologyReaderException,
      SNEEDataSourceException, CostParametersException, 
      SNCBException, SNEECompilerException, 
      IOException, CodeGenerationException;

  public abstract void runCostModels() throws OptimizationException;

  public abstract void runAnyliserWithDeadNodes() throws OptimizationException;

  public abstract void setDeadNodes(ArrayList<Integer> deadNodes);

  public abstract void setNoDeadNodes(int noDeadNodes);

  public abstract float getCECMEpochResult() throws OptimizationException;

  public abstract float getCECMAgendaResult() throws OptimizationException;

  public abstract void callAnaysliserAnaylsisSNEECard(
      Map<Integer, Integer> sneeTuplesPerEpoch);

  public abstract void queryEnded();
  
  public abstract void initilise(SourceMetadataAbstract _metadata, QueryExecutionPlan queryPlan, 
                                 ResultStore resultSet) 
  throws SNEEException, SNEEConfigurationException, 
  SchemaMetadataException, TypeMappingException, 
  OptimizationException, IOException;

  //no tuples received this query
  public abstract void callAnaysliserAnaylsisSNEECard();

  public abstract void setQuery(String query);

  public abstract File getOutputFolder();
  
  public abstract void setListener(SNCBSerialPortReceiver mr);
  
  public abstract void runSimulatedNodeFailure()throws OptimizationException,
  SNEEConfigurationException, SchemaMetadataException,
  TypeMappingException, AgendaException, SNEEException,
  MalformedURLException, MetadataException,
  UnsupportedAttributeTypeException, SourceMetadataException,
  TopologyReaderException, SNEEDataSourceException,
  CostParametersException, SNCBException, 
  SNEECompilerException, IOException, CodeGenerationException;
}