package uk.ac.manchester.cs.snee;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collection;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;


public interface SNEE {

	/**
	 * Adds a service source to the set of available data for querying.
	 * @param name name of the service
	 * @param url endpoint reference for the interface
	 * @param interfaceType type of the interface
	 * @throws MalformedURLException
	 * @throws MetadataException
	 * @throws SNEEDataSourceException 
	 */
	public void addServiceSource(String name, String url, 
			SourceType interfaceType) 
	throws MalformedURLException, MetadataException,
	SNEEDataSourceException;
	
	/**
	 * Return a list of the extent names available in the schema
	 * @return list of available extents
	 */
	public Collection<String> getExtentNames();
	
	/**
	 * Retrieve the metadata about a specified extent.
	 * @param extentName name of the extent
	 * @return details about the extent
	 * @throws MetadataException name is not an extent in the schema
	 */
	public ExtentMetadata getExtentDetails(String extentName) 
	throws MetadataException;
	
	/**
	 * Adds a query to the set of registered queries and returns the 
	 * generated query identifier. 
	 * 
	 * It takes a query statement as input, and optionally any 
	 * parameters associated with it, generates a query plan for its 
	 * evaluation, and adds it to the set of registered query plans.
	 * 
	 * @param query Statement of the query
	 * @param parametersFile location of the query parameters file
	 *
	 * @throws SNEEException
	 * @throws MetadataException 
	 * @throws EvaluatorException 
	 * @throws SNEECompilerException 
	 * @throws SNEEConfigurationException 
	 * @throws IOException 
	 * @throws OptimizationException 
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws CodeGenerationException 
	 */
	public int addQuery(String query, String parametersFile) 
	throws SNEECompilerException, MetadataException, 
	EvaluatorException, SNEEException, SNEEConfigurationException,
	SchemaMetadataException, TypeMappingException, OptimizationException, 
	IOException, CodeGenerationException;

	/**
   * Adds a query to the set of registered queries and returns the 
   * generated query identifier. 
   * 
   * It takes a query statement as input, and optionally any 
   * parameters associated with it, generates a query plan for its 
   * evaluation, and adds it to the set of registered query plans.
   * 
   * @param query Statement of the query
   * @param parametersFile location of the query parameters file
   * @param queryID id for this query
   *
   * @throws SNEEException
   * @throws MetadataException 
   * @throws EvaluatorException 
   * @throws SNEECompilerException 
   * @throws SNEEConfigurationException 
	 * @throws IOException 
	 * @throws OptimizationException 
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws CodeGenerationException 
   */
	
	public int addQuery(String query, String parametersFile, Integer queryID, 
	                    boolean compilation, boolean starting, boolean dispatching)
	throws SNEECompilerException, MetadataException, 
  EvaluatorException, SNEEException, SNEEConfigurationException, 
  SchemaMetadataException, TypeMappingException, OptimizationException, 
  IOException, CodeGenerationException;
	
  /**
   * Adds a query to the set of registered queries and returns the 
   * generated query identifier. 
   * 
   * It takes a query statement as input, and optionally any 
   * parameters associated with it, does not go though the compilation stages
   * , and adds it to the set of registered query plans.
   * 
   * @param query Statement of the query
   * @param parametersFile location of the query parameters file
   * @param queryID id for this query
   *
   * @throws SNEEException
   * @throws MetadataException 
   * @throws EvaluatorException 
   * @throws SNEECompilerException 
   * @throws SNEEConfigurationException 
   * @throws IOException 
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws CodeGenerationException 
   */	
	
	public ResultStore getResultStore(int queryId) 
	throws SNEEException;
	
	/**
	 * Removes a query from the set of registered queries. It takes a query 
	 * identifier and stops the required query evaluation if it exists.
	 * 
	 * @param queryId Identifier of the query to be stopped
	 * @throws SNEEException An exception is thrown if the query identifier does not exist
	 */
	public void removeQuery(int queryId) 
	throws SNEEException;

	/**
	 * Close SNEE down gracefully
	 */
	public void close();

	/**
	 * allows the monitor to simulate the energy drain of 
	 * Numerous agenda execution cycles, without going though avrora
	 * @param fixedNumberOfAgendaExecutionCycles
	 */
  public void simulateEnergyDrainofAganedaExecutionCycles(int fixedNumberOfAgendaExecutionCycles);

}