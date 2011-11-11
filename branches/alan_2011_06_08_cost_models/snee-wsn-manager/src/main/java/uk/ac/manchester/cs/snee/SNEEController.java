/****************************************************************************\
*                                                                            *
*  SNEE (Sensor NEtwork Engine)                                              *
*  http://snee.cs.manchester.ac.uk/                                          *
*  http://code.google.com/p/snee                                             *
*                                                                            *
*  Release 1.x, 2009, under New BSD License.                                 *
*                                                                            *
*  Copyright (c) 2009, University of Manchester                              *
*  All rights reserved.                                                      *
*                                                                            *
*  Redistribution and use in source and binary forms, with or without        *
*  modification, are permitted provided that the following conditions are    *
*  met: Redistributions of source code must retain the above copyright       *
*  notice, this list of conditions and the following disclaimer.             *
*  Redistributions in binary form must reproduce the above copyright notice, *
*  this list of conditions and the following disclaimer in the documentation *
*  and/or other materials provided with the distribution.                    *
*  Neither the name of the University of Manchester nor the names of its     *
*  contributors may be used to endorse or promote products derived from this *
*  software without specific prior written permission.                       *
*                                                                            *
*  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS   *
*  IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, *
*  THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR    *
*  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR          *
*  CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,     *
*  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,       *
*  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR        *
*  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF    *
*  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING      *
*  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS        *
*  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.              *
*                                                                            *
\****************************************************************************/

package uk.ac.manchester.cs.snee;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.QueryCompiler;
import uk.ac.manchester.cs.snee.compiler.params.QueryParameters;
import uk.ac.manchester.cs.snee.compiler.params.qos.QoSExpectations;
import uk.ac.manchester.cs.snee.compiler.queryplan.AgendaException;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.QueryExecutionPlanAbstract;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.evaluator.Dispatcher;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerException;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceDoesNotExistException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;
import uk.ac.manchester.cs.snee.sncb.SNCB;
import uk.ac.manchester.cs.snee.sncb.SNCBException;
import uk.ac.manchester.cs.snee.sncb.TinyOS_SNCB_Controller;


/**
 * Controller class for SNEEql query compilation and evaluation
 * Compiles a declarative SNEEql query and controls the execution of it 
 * over the available sensor network and robust networks.
 */
public class SNEEController implements SNEE {
//FIXME: This should really be in the core module with interfaces used for the various snee modules
	private static final Logger logger = Logger.getLogger(SNEEController.class.getName());
	
	/**
	 * Sensor Network Connectivity Bridge.  For now, assume that there 
	 * is one instance max.
	 */
	private SNCB _sncb = null;
		
	/**
	 * Metadata stored about extents, data sources and cost parameters.
	 */
	private MetadataManager _metadata;
	
	/**
	 * The compiler object for compiling queries
	 */
	private QueryCompiler _compiler;
	
	/**
	 * The evaluator object for running queries
	 */
	private Dispatcher _dispatcher;

	/**
	 * Stores the results for each registered query
	 */
	private Map<Integer, ResultStore> _queryResults = 
		new HashMap<Integer, ResultStore>();
	
	/**
	 * Stores the query plan for each registered query
	 */
	private Map<Integer, QueryExecutionPlanAbstract> _queryPlans = 
		new HashMap<Integer, QueryExecutionPlanAbstract>();
	
	private static int _nextQueryID = 1;

	private double duration;
	
	/**
	 * Initialise SNEE based on the contents of the configuration files
	 * @throws SNEEException 
	 * @throws SNEEConfigurationException 
	 */
	public SNEEController() 
	throws SNEEException, SNEEConfigurationException {
		if (logger.isInfoEnabled())
			logger.info("ENTER SNEEController()");
		InputStream propsStream = 
			this.getClass().getResourceAsStream("snee.properties");
		Properties props = new Properties();
		try {
			props.load(propsStream);
			propsStream.close();
			SNEEProperties.initialise(props);
			initialise();
		} catch (IOException e) {
			logger.error("Could not load properties file.");
			throw new SNEEConfigurationException(e);
		} 
		if (logger.isInfoEnabled())
			logger.info("RETURN SNEEController()");
	}
	
	public SNEEController(String propertiesFilename) 
	throws SNEEException, SNEEConfigurationException {
		if (logger.isInfoEnabled())
			logger.info("ENTER SNEEController() properties file " + propertiesFilename);
		logger.info(this.getClass().getClassLoader().getResource("/"));
		InputStream propsStream = 
			this.getClass().getClassLoader().getResourceAsStream(propertiesFilename);
		logger.trace("Creating properties table");
		Properties props = new Properties();
		try {
			logger.trace("Loading properties");
			props.load(propsStream);
			propsStream.close();
			SNEEProperties.initialise(props);
			initialise();
		} catch (IOException e) {
			logger.error("Could not load properties file.");
			throw new SNEEConfigurationException(e);
		} 
		if (logger.isInfoEnabled())
			logger.info("RETURN SNEEController()");
	}
	 
  public SNEEController(String propertiesFilename, double duration) 
  throws SNEEException, SNEEConfigurationException {
    if (logger.isInfoEnabled())
      logger.info("ENTER SNEEController() properties file " + propertiesFilename);
    logger.info(this.getClass().getClassLoader().getResource("/"));
    InputStream propsStream = 
      this.getClass().getClassLoader().getResourceAsStream(propertiesFilename);
    this.duration = duration;
    logger.trace("Creating properties table");
    Properties props = new Properties();
    try {
      logger.trace("Loading properties");
      props.load(propsStream);
      propsStream.close();
      SNEEProperties.initialise(props);
      initialise();
    } catch (IOException e) {
      logger.error("Could not load properties file.");
      throw new SNEEConfigurationException(e);
    } 
    if (logger.isInfoEnabled())
      logger.info("RETURN SNEEController()");
  }
  
	public SNEEController(Properties props) 
	throws SNEEException, SNEEConfigurationException {
		if (logger.isInfoEnabled())
			logger.info("ENTER SNEEController(Properties props)");
		SNEEProperties.initialise(props);
		initialise();
		if (logger.isInfoEnabled())
			logger.info("RETURN SNEEController(Properties props)");
	}

	/**
	 * Method used by public constructors to initialise the logger,
	 * and local modules (metadata, compiler, and query evaluator).
	 * 
	 * @throws SNEEException
	 * @throws SNEEConfigurationException 
	 */
	private void initialise() 
	throws SNEEException, SNEEConfigurationException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER initialise()");

		try {
			
			_sncb = initialiseSNCB(duration);
			
			/* Process metadata */
			_metadata = initialiseMetadata();
			
			/* Initialise compiler */
			_compiler = initialiseQueryCompiler();
			
			/* Initialise dispatcher */
			_dispatcher = initialiseDispatcher();

		} catch (TypeMappingException e) {
			String msg = "Problem instantiating logical schema " + 
				SNEEProperties.getSetting(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE) + ". " + e;
			logger.fatal(msg);
			throw new SNEEException(msg, e);
		} catch (SchemaMetadataException e) {
			String msg = "Problem instantiating logical schema " + 
				SNEEProperties.getSetting(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE) + ". " + e;
			logger.fatal(msg);
			throw new SNEEException(msg, e);
		} catch (SourceMetadataException e) {
			String msg = "Problem instantiating physical schema " + 
				SNEEProperties.getSetting(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE) + ". " + e;
			logger.fatal(msg);
			throw new SNEEException(msg, e);
		} catch (MetadataException e) {
			String msg = "Problem instantiating schema " +
				SNEEProperties.getSetting(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE) + ". " + e;
			logger.fatal(msg);
			throw new SNEEException(msg, e);
		} catch (UnsupportedAttributeTypeException e) {
			String msg = "Problem instantiating logical schema " + 
				SNEEProperties.getSetting(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE) + ". " + e;
			logger.fatal(msg, e);
			throw new SNEEException(msg, e);
		} catch (TopologyReaderException e) {
			String msg = "Problem reading topology file ";
			logger.fatal(msg, e);
			throw new SNEEException(msg, e);			
		} catch (MalformedURLException e) {
			String msg = "Problem creating link to external service ";
			logger.fatal(msg, e);
			throw new SNEEException(msg, e);			
		} catch (SNEEDataSourceException e) {
			String msg = "Problem creating link to external service ";
			logger.fatal(msg, e);
			throw new SNEEException(msg, e);			
		} catch (CostParametersException e) {
			String msg = "Problem reading cost parameters file";
			logger.fatal(msg, e);
			throw new SNEEException(msg, e);
		} catch (SNCBException e) {
			String msg = "Problem intialising sensor network";
			logger.fatal(msg, e);
			throw new SNEEException(msg, e);
		}
		
		logger.info("SNEE configured");
		if (logger.isDebugEnabled())
			logger.debug("RETURN initialise()");
	}

  protected SNCB initialiseSNCB(double duration) throws SNEEConfigurationException, SNCBException 
  {
		if (logger.isTraceEnabled())
			logger.trace("ENTER initialiseSNCB()");
		
		return new TinyOS_SNCB_Controller(duration);
	}
	
	protected MetadataManager initialiseMetadata() 
	throws MetadataException, SchemaMetadataException, 
	TypeMappingException, UnsupportedAttributeTypeException, 
	SourceMetadataException, SNEEConfigurationException, 
	TopologyReaderException, MalformedURLException,
	SNEEDataSourceException, CostParametersException, SNCBException 
	{
		if (logger.isTraceEnabled())
			logger.trace("ENTER initialiseMetadata()");
		MetadataManager metadata = new MetadataManager(_sncb);
		return metadata;
	}

	protected QueryCompiler initialiseQueryCompiler() 
	throws TypeMappingException 
	{
		return new QueryCompiler(_metadata);
	}

	protected Dispatcher initialiseDispatcher() 
	{
		return new Dispatcher(_metadata);
	}
	
	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.SNEE#getExtents()
	 */
	public Collection<String> getExtentNames() 
	{
		if (logger.isDebugEnabled())
			logger.debug("ENTER getExtents()");
		Collection<String> extentNames = _metadata.getExtentNames();
		if (logger.isDebugEnabled())
			logger.debug("RETURN getExtents() #extents=" + 
					extentNames.size());
		return extentNames;
	}
	
	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.SNEE#getExtentDetails(java.lang.String)
	 */
	public ExtentMetadata getExtentDetails(String extentName) 
	throws ExtentDoesNotExistException 
	{
		if (logger.isDebugEnabled())
			logger.debug("ENTER getExtentDetails() with " + extentName);
		ExtentMetadata extent = _metadata.getExtentMetadata(extentName);
		if (logger.isDebugEnabled())
			logger.debug("RETURN getExtentDetails()");
		return extent;
	}
	
	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.SNEE#addQuery(java.lang.String)
	 */
	public int addQuery(String query, String queryParamsFile) 
	throws EvaluatorException, SNEECompilerException, SNEEException,
	MetadataException, SNEEConfigurationException,
	SchemaMetadataException, TypeMappingException, 
	OptimizationException, IOException, CodeGenerationException 
	{
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER addQuery() with " + query);
		}
		if (query == null || query.trim().equals("")) {
			logger.warn("Null or empty query passed in");
			throw new SNEECompilerException("Null or empty query passed in.");
		}
		int queryId = getNextQueryId();
		if (logger.isInfoEnabled()) 
			logger.info("Assigned ID " + queryId + " to query\n\t" + query);
		if (logger.isInfoEnabled()) 
			logger.info("Reading query " + queryId + " parameters\n");
		QueryParameters queryParams = null;
		if (queryParamsFile != null) {
			try {
				queryParams = new QueryParameters(queryId, queryParamsFile);
			} catch (Exception e) {
				logger.warn("Error obtaining query parameters: " + e);
				throw new SNEECompilerException(e.getLocalizedMessage());
			}
		}
		if (logger.isInfoEnabled()) 
			logger.info("Compiling query " + queryId + "\n");
		compileQuery(queryId, query, queryParams);
		if (logger.isInfoEnabled())
			logger.info("Successfully compiled query " + queryId);	
		dispatchQuery(queryId, query, queryParams);
		if (logger.isInfoEnabled())
			logger.info("Successfully started evaluation of query " + queryId);

		if (logger.isDebugEnabled()) {
    		logger.debug("RETURN addQuery() with query id " + queryId);
    	}
		return queryId;
	}
	
  public int queryCompilationOnly(String query, String queryParamsFile) 
  throws SNEECompilerException
  {
    if (logger.isDebugEnabled()) {
      logger.debug("ENTER addQuery() with " + query);
    }
    if (query == null || query.trim().equals("")) {
      logger.warn("Null or empty query passed in");
      throw new SNEECompilerException("Null or empty query passed in.");
    }
    int queryId = getNextQueryId();
    if (logger.isInfoEnabled()) 
      logger.info("Assigned ID " + queryId + " to query\n\t" + query);
    if (logger.isInfoEnabled()) 
      logger.info("Reading query " + queryId + " parameters\n");
    QueryParameters queryParams = null;
    if (queryParamsFile != null) {
      try {
        queryParams = new QueryParameters(queryId, queryParamsFile);
      } catch (Exception e) {
        logger.warn("Error obtaining query parameters: " + e);
        throw new SNEECompilerException(e.getLocalizedMessage());
      }
    }
    if (logger.isInfoEnabled()) 
      logger.info("Compiling query " + queryId + "\n");
    compileQuery(queryId, query, queryParams);
    return queryId;
  }
	
  public int addQueryWithoutCompilation(String query, String queryParamsFile)
  throws EvaluatorException, SNEECompilerException, SNEEException,
  MetadataException, SNEEConfigurationException,
  SchemaMetadataException, TypeMappingException, 
  OptimizationException, IOException, CodeGenerationException 
  {
    if (logger.isDebugEnabled()) {
      logger.debug("ENTER addQuery() with " + query);
    }
    if (query == null || query.trim().equals("")) {
      logger.warn("Null or empty query passed in");
      throw new SNEECompilerException("Null or empty query passed in.");
    }
    int queryId = _nextQueryID - 1;
    QueryParameters queryParams = null;
    if (queryParamsFile != null) {
      try {
        queryParams = new QueryParameters(queryId, queryParamsFile);
      } catch (Exception e) {
        logger.warn("Error obtaining query parameters: " + e);
        throw new SNEECompilerException(e.getLocalizedMessage());
      }
    }
    dispatchQuery(queryId, query, queryParams);
    if (logger.isInfoEnabled())
      logger.info("Successfully started evaluation of query " + queryId);

    if (logger.isDebugEnabled()) {
        logger.debug("RETURN addQuery() with query id " + queryId);
      }
    return queryId;
  }
  
  public int addQueryWithoutCompilationAndStarting(String query, String queryParamsFile) 
  throws 
  SNEECompilerException, MalformedURLException, 
  SNEEException, MetadataException, EvaluatorException, 
  SNEEConfigurationException, OptimizationException, 
  SchemaMetadataException, TypeMappingException, 
  AgendaException, UnsupportedAttributeTypeException, 
  SourceMetadataException, TopologyReaderException, 
  SNEEDataSourceException, CostParametersException, 
  SNCBException, IOException, CodeGenerationException
  {
    if (logger.isDebugEnabled()) {
      logger.debug("ENTER addQuery() with " + query);
    }
    if (query == null || query.trim().equals("")) {
      logger.warn("Null or empty query passed in");
      throw new SNEECompilerException("Null or empty query passed in.");
    }
    int queryId = _nextQueryID - 1;
    dispatchQueryWithoutStarting(queryId, query);
    
    if (logger.isInfoEnabled())
      logger.info("Successfully started evaluation of query " + queryId);

    if (logger.isDebugEnabled()) {
        logger.debug("RETURN addQuery() with query id " + queryId);
      }
    return queryId;
  }
  
	public int addQuery(String query, String queryParamsFile, int currentQueryID)
	throws EvaluatorException, SNEECompilerException, SNEEException,
  MetadataException, SNEEConfigurationException, SchemaMetadataException, 
  TypeMappingException, OptimizationException, IOException, CodeGenerationException
	{
	  if (logger.isDebugEnabled()) {
      logger.debug("ENTER addQuery() with " + query);
    }
    if (query == null || query.trim().equals("")) {
      logger.warn("Null or empty query passed in");
      throw new SNEECompilerException("Null or empty query passed in.");
    }
    int queryId = currentQueryID;
    _nextQueryID = currentQueryID+1;
    if (logger.isInfoEnabled()) 
      logger.info("Assigned ID " + queryId + " to query\n");
    if (logger.isInfoEnabled()) 
      logger.info("Reading query " + queryId + " parameters\n");
    QueryParameters queryParams = null;
    if (queryParamsFile != null) {
      try {
        queryParams = new QueryParameters(queryId, queryParamsFile);
      } catch (Exception e) {
        logger.warn("Error obtaining query parameters: " + e);
        throw new SNEECompilerException(e.getLocalizedMessage());
      }
    }
    if (logger.isInfoEnabled()) 
      logger.info("Compiling query " + queryId + "\n");
    compileQuery(queryId, query, queryParams);
    if (logger.isInfoEnabled())
      logger.info("Successfully compiled query " + queryId);  
    dispatchQuery(queryId, query, queryParams);
    if (logger.isInfoEnabled())
      logger.info("Successfully started evaluation of query " + queryId);

    if (logger.isDebugEnabled()) {
        logger.debug("RETURN addQuery() with query id " + queryId);
      }
    return queryId;
	}
	
	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.SNEE#removeQuery(int)
	 */
	public void removeQuery(int queryId) throws SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER removeQuery() with " + queryId);
		}
		_dispatcher.stopQuery(queryId);
		if (logger.isDebugEnabled()) {
    		logger.debug("RETURN removeQuery()");
    	}
	}
		
	/**
	 * Dispatch the query for evaluation
	 * @param query 
	 * @param queryParams 
	 * 
	 * @return the query identifier generated for the query
	 * @throws SNEEException Problem starting the query evaluation
	 * @throws SchemaMetadataException 
	 * @throws EvaluatorException 
	 * @throws CodeGenerationException 
	 * @throws OptimizationException 
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws IOException 
	 * @throws SNEEConfigurationException 
	 * @throws CodeGenerationException 
	 * @throws OptimizationException 
	 * @throws TypeMappingException 
	 * @throws SchemaMetadataException 
	 * @throws IOException 
	 * @throws SNEEConfigurationException 
	 */
	private int dispatchQuery(int queryId, String query, QueryParameters queryParams) 
	throws SNEEException, MetadataException, EvaluatorException,
	SNEEConfigurationException, SchemaMetadataException, 
	TypeMappingException, OptimizationException, IOException, CodeGenerationException
	{
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER dispatchQuery() with " + queryId +
					" " + query);
		}
		QueryExecutionPlan queryPlan = _queryPlans.get(queryId);
		ResultStore resultSet = createStreamResultSet(query, queryPlan);
		_dispatcher.initiliseAutonomicManager(queryId, resultSet, queryPlan);
		_dispatcher.giveAutonomicManagerQuery(query);
		_dispatcher.giveAutonomicManagerQueryParams(queryParams);
		_dispatcher.startQuery(queryId, resultSet, queryPlan);
		_queryResults.put(queryId, resultSet);
		
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN dispatchQuery() with queryId " + queryId);
		}
		return queryId;
	}
	
	private int dispatchQueryWithoutStarting(int queryId, String query) 
  throws
  SNEEException, MetadataException, EvaluatorException,
  SNEEConfigurationException, MalformedURLException, 
  OptimizationException, SchemaMetadataException, 
  TypeMappingException, AgendaException,
  UnsupportedAttributeTypeException, SourceMetadataException, 
  TopologyReaderException, SNEEDataSourceException, 
  CostParametersException, SNCBException, 
  SNEECompilerException, IOException, 
  CodeGenerationException
  {
    if (logger.isTraceEnabled()) {
      logger.trace("ENTER dispatchQuery() with " + queryId +
          " " + query);
    }
    QueryExecutionPlan queryPlan = _queryPlans.get(queryId);
    ResultStore resultSet = createStreamResultSet(query, queryPlan);
    _dispatcher.initiliseAutonomicManager(queryId, resultSet, queryPlan);
    _dispatcher.giveAutonomicManagerQuery(query);
    
    if (logger.isTraceEnabled()) {
      logger.trace("RETURN dispatchQuery() with queryId " + queryId);
    }
    return queryId;
  }

	protected ResultStore createStreamResultSet(String query,
			QueryExecutionPlan queryPlan) 
	throws SNEEException, SNEEConfigurationException {
		ResultStore resultSet = new ResultStoreImpl(query, queryPlan);
		resultSet.setCommand(query);
		return resultSet;
	}

	private int getNextQueryId() {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER getNextQueryId()");
		}
		int queryId = _nextQueryID;
		_nextQueryID++;
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN getNextQueryId() with queryId " + 
					queryId);
		}
		return queryId;
	}

	private void compileQuery(int queryID, String query, 
			QueryParameters queryParams) 
	throws SNEECompilerException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER compilerQuery() with queryID " + 
					queryID + "\n\tquery: " + query);
		}
			try {
				QoSExpectations qos = null;
				if (queryParams!=null) {
					qos = queryParams.getQoS();
				}
				QueryExecutionPlanAbstract queryPlan = 
					_compiler.compileQuery(queryID, query, qos);
				_queryPlans.put(queryID, queryPlan);
			} catch (Exception e) {
				String msg = "Problem compiling query: "+
					e.getLocalizedMessage()+"\n ";
				logger.warn(msg, e);
				throw new SNEECompilerException(msg, e);
			}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN compileQuery()");
		}
	}

	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.SNEE#close()
	 */
	public void close() {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER close()");
		}		
		// Stop the query evaluator
		_dispatcher.close();
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN close()");
		}
	}

	public ResultStore getResultStore(int queryId) 
	throws SNEEException {
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER getResultStore() with query=" + queryId);
		}
		ResultStore resultSet;
		if (_queryResults.containsKey(queryId)) {
			resultSet = _queryResults.get(queryId);
		} else {
			String msg = "No ResultStore for query " + queryId;
			logger.warn(msg);
			throw new SNEEException(msg);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN getResultStore()");
		}
		return resultSet;
	}

	/* (non-Javadoc)
	 * @see uk.ac.manchester.cs.snee.SNEE#addServiceSource(java.lang.String)
	 */
	public void addServiceSource(String name, String url, 
			SourceType interfaceType) 
	throws MalformedURLException, SNEEDataSourceException,
	MetadataException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER addServiceSource() with name=" +
					name + " type=" + interfaceType + " url="+ url);
		try {
			_metadata.addDataSource(name, url, 
					interfaceType);
		} catch (SchemaMetadataException e) {
			logger.warn("Throwing a MetadataException. Cause " + e);
			throw new MetadataException(e.getLocalizedMessage());
		} catch (TypeMappingException e) {
			logger.warn("Throwing a MetadataException. Cause " + e);
			throw new MetadataException(e.getLocalizedMessage());
		} catch (SourceMetadataException e) {
			logger.warn("Throwing a MetadataException. Cause " + e);
			throw new MetadataException(e.getLocalizedMessage());
		}
		logger.info("Web service source added with url \n\t" + url);
		if (logger.isDebugEnabled())
			logger.debug("RETURN addServiceSource()");

	}
	
	
	
	public void giveAutonomicManagerQuery(String query)
	{
	  _dispatcher.giveAutonomicManagerQuery(query);
	}
	
	public void runSimulatedNodeFailure(ArrayList<String> failedNodesID) 
	throws 
	MalformedURLException, SNEEConfigurationException, 
	OptimizationException, SchemaMetadataException, 
	TypeMappingException, AgendaException, SNEEException, 
	MetadataException, UnsupportedAttributeTypeException, 
	SourceMetadataException, TopologyReaderException, 
	SNEEDataSourceException, CostParametersException, 
	SNCBException, SNEECompilerException, IOException, 
	CodeGenerationException, AutonomicManagerException
	{
	  _dispatcher.runSimulatedNodeFailure(failedNodesID);
	}
	
	public MetadataManager getMetaData()
	{
	  return _metadata;
	}
	
	public void setDeadNodes(ArrayList<String> deadNodes)
	{
	  _dispatcher.setDeadNodes(deadNodes);
	}
	
	public void setNoDeadNodes(int noDeadNodes)
	{
	  _dispatcher.setNoDeadNodes(noDeadNodes);
	}
	
	public QueryExecutionPlan getQEP()
	{
		return _dispatcher.getQEP();
	}
	
	public void waitForQueryEnd() throws InterruptedException
	{
	  _dispatcher.waitForQueryEnd();
	}
  
  public void resetQueryId()
  {
	 _nextQueryID = 1;
  }
  
  public void setQueryID(int newqueryID)
  {
    _nextQueryID = newqueryID;
  }

  @Override
  public void simulateEnergyDrainofAganedaExecutionCycles(int fixedNumberOfAgendaExecutionCycles)
  {
    _dispatcher.simulateEnergyDrainofAganedaExecutionCycles(fixedNumberOfAgendaExecutionCycles);
  }

  public void resetMetaData(SensorNetworkQueryPlan qep) 
  throws SourceDoesNotExistException, SourceMetadataException, 
  SNEEConfigurationException, SNCBException, TopologyReaderException
  {
    SourceMetadataAbstract metadata = 
      _metadata.getSource(qep.getMetaData().getOutputAttributes().get(1).getExtentName());
    SensorNetworkSourceMetadata sm = (SensorNetworkSourceMetadata) metadata;
    sm.resetSources();
    sm.resetTopology();
  }

  public void resetQEP(SensorNetworkQueryPlan qep)
  {
    _dispatcher.resetQEP(qep);
    
  }

  public void removeNodeFromTheMetaData(String failedID, SensorNetworkQueryPlan qep) 
  throws SourceDoesNotExistException
  {
    SourceMetadataAbstract metadata = 
      _metadata.getSource(qep.getMetaData().getOutputAttributes().get(1).getExtentName());
    SensorNetworkSourceMetadata sm = (SensorNetworkSourceMetadata) metadata;
    sm.removeSourceSite(new Integer(failedID));
    sm.removeNodeFromTopology(failedID);  
  }
}
