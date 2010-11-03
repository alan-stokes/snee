/****************************************************************************\ 
 *                                                                            *
 *  SNEE (Sensor NEtwork Engine)                                              *
 *  http://code.google.com/p/snee                                             *
 *  Release 1.0, 24 May 2009, under New BSD License.                          *
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
package uk.ac.manchester.cs.snee.metadata;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.common.Constants;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.IDAttribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.TimeAttribute;
import uk.ac.manchester.cs.snee.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentDoesNotExistException;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentMetadata;
import uk.ac.manchester.cs.snee.metadata.schema.ExtentType;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.Types;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceManager;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.SourceType;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;

public class MetadataManager {

	private Logger logger = 
		Logger.getLogger(MetadataManager.class.getName());
	
	private SourceManager _sources;

	/**
	 * Details of the logical schema. Extent name used as the
	 * key by which to extract schema metadata items
	 */
	private Map<String, ExtentMetadata> _schema =
		new HashMap<String, ExtentMetadata>();

	/**
	 * Cost Parameters
	 */
	private CostParameters costParams;
	
	private Types _types;

	private AttributeType timeType;

	private AttributeType idType;

	/**
	 * Metadata about logical and physical schema
	 * @throws TypeMappingException
	 * @throws SchemaMetadataException
	 * @throws SNEEConfigurationException Problem reading configuration from properties file
	 * @throws UnsupportedAttributeTypeException 
	 * @throws MetadataException 
	 * @throws SourceMetadataException 
	 * @throws TopologyReaderException 
	 * @throws MalformedURLException 
	 * @throws SNEEDataSourceException 
	 * @throws CostParametersException 
	 */
	public MetadataManager() 
	throws TypeMappingException, SchemaMetadataException, 
	SNEEConfigurationException, MetadataException, 
	UnsupportedAttributeTypeException, SourceMetadataException, 
	TopologyReaderException, MalformedURLException,
	SNEEDataSourceException, CostParametersException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER MetadataManger()");
		logger.trace("Processing types.");
		String typesFile = 
			SNEEProperties.getFilename(SNEEPropertyNames.INPUTS_TYPES_FILE);
		_types = new Types(typesFile);
		timeType = _types.getType(Constants.TIME_TYPE);
		timeType.setSorted(true);
		idType = _types.getType(Constants.ID_TYPE);
		logger.trace("Processing logical schema.");
		if (SNEEProperties.isSet(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE)) {
			processLogicalSchema(
					SNEEProperties.getFilename(SNEEPropertyNames.INPUTS_LOGICAL_SCHEMA_FILE));
		}
		logger.trace("Processing physical schema.");
		_sources = createSourceManager();
		if (SNEEProperties.isSet(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE)) {
			_sources.processPhysicalSchema(
					SNEEProperties.getFilename(SNEEPropertyNames.INPUTS_PHYSICAL_SCHEMA_FILE));
		}
		//cardinalities for SN sources can only be estimated after physical schema
		//known, as they depend on the number of source sites.
		logger.trace("Estimating extent cardinalities.");
		doCardinalityEstimations();
		logger.trace("Getting cost estimation model parameters.");
		if (SNEEProperties.isSet(SNEEPropertyNames.INPUTS_COST_PARAMETERS_FILE)) {
			this.costParams = new CostParameters(SNEEProperties.getFilename(
					SNEEPropertyNames.INPUTS_COST_PARAMETERS_FILE));
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN MetadataManager()");
	}

	/**
	 * Method used to create source manager in order to enable
	 * mocking while testing
	 * @return
	 */
	private SourceManager createSourceManager() {
		return new SourceManager(_schema, _types);
	}

	private void processLogicalSchema(String logicalSchemaFile) 
	throws SchemaMetadataException, MetadataException, 
	TypeMappingException, UnsupportedAttributeTypeException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER processLogicalSchema() with " +
					logicalSchemaFile);
		Document doc = parseFile(logicalSchemaFile);
		Element root = (Element) doc.getFirstChild();
		/* Process stream extents */
		NodeList xmlSources = root.getElementsByTagName("stream");
		for (int i = 0; i < xmlSources.getLength(); i++) {
			Element element = (Element) xmlSources.item(i);
			String typeAttr = element.getAttribute("type");
			if (typeAttr.equals("pull")) {				
				/* Process pull streams */
				addExtent(ExtentType.SENSED, element);
			} else if (typeAttr.equals("push")) {
				/* Process push streams */
				addExtent(ExtentType.PUSHED, element);
			}
		}				
		/* Process stored relations */
		xmlSources = root.getElementsByTagName("table");
		for (int i = 0; i < xmlSources.getLength(); i++) {
			Element element = (Element) xmlSources.item(i);
			addExtent(ExtentType.TABLE, element);
		}				
		/* Log a warning for the use of the old declaration of stream */
		xmlSources = root.getElementsByTagName("stream");
		if (xmlSources.getLength() > 0) {
			logger.warn("Old schema declaration used for stream. " +
					"You should update your schema to use " +
			"push_stream or pull_stream as appropriate.");
		}	
		if (logger.isInfoEnabled())
			logger.info("Logical schema successfully read in from " + 
					logicalSchemaFile + ". Number of extents=" + 
					_schema.size() + "\n\tExtents: " + _schema.keySet());
		if (logger.isTraceEnabled()) {
			logger.trace("Available extents:\n\t" + _schema.keySet());
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN processLogicalSchema() #extents=" + 
					_schema.size());
	}

	/**
	 * Add a given extent object to the schema 
	 * @param extent the object that represents the extent
	 * @throws MetadataException extent with the same name exists
	 */
	public void addExtent(ExtentMetadata extent) 
	throws MetadataException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER addExtent() with " + extent);
		String extentName = extent.getExtentName();
		if (_schema.containsKey(extentName)) {
			String message = "Extent " + extentName + " already exists.";
			logger.warn(message);
			throw new MetadataException(message);
		}
		_schema.put(extentName, extent);
		if (logger.isTraceEnabled()) {
			logger.trace("Extent " + extentName + 
			" successfully added to the schema.");
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN addExtent()");
	}

	/**
	 * Generate an ExtentMetadata object to represent logical extent
	 * 
	 * @param extentType Type of data source extent: sensed, pushed, or table 
	 * @param element XML element for the source metadata 
	 * @throws SchemaMetadataException 
	 * @throws TypeMappingException 
	 */
	private void addExtent(ExtentType extentType, Element element) 
	throws TypeMappingException, SchemaMetadataException 
	{
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER addExtent() of type " + extentType);
		}
		String extentName = element.getAttribute("name").toLowerCase();
		logger.trace("extentName="+extentName);
		List<Attribute> attributes = 
			parseAttributes(element.getElementsByTagName("column"),
					extentName);
		if (extentType == ExtentType.SENSED) {
			attributes.add(0, new IDAttribute(extentName, 
					Constants.ACQUIRE_ID, idType));
			attributes.add(1, new TimeAttribute(extentName, 
					Constants.ACQUIRE_TIME, timeType));
		}
		ExtentMetadata extent =
			new ExtentMetadata(extentName, attributes, extentType);
		_schema.put(extentName, extent);
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN addExtent() " + extentName);
		}
	}

	private List<Attribute> parseAttributes(NodeList columns, 
			String extentName) 
	throws TypeMappingException, SchemaMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER parseAttributes() number of columns " + 
					columns.getLength());
		List<Attribute> attributes =
			new ArrayList<Attribute>();
		for (int i = 0; i < columns.getLength(); i++) {
			Element tabElement = (Element) columns.item(i);
			String attributeName = 
				tabElement.getAttribute("name").toLowerCase();
			if (logger.isTraceEnabled()) 
				logger.trace("Reading attribute " + attributeName);
			NodeList xmlType = tabElement.getElementsByTagName("type");
			String sqlType = 
				((Element) xmlType.item(0)).getAttribute("class").toLowerCase();
			if (logger.isTraceEnabled()) 
				logger.trace("sqlType = " + sqlType);
			AttributeType type = _types.getType(sqlType);
			if (logger.isTraceEnabled()) 
				logger.trace("Type = " + type);
			type.setLength(((Element) xmlType.item(0)).getAttribute("length"));
			attributes.add(new DataAttribute(extentName, 
					attributeName, type));
		}
		if (logger.isTraceEnabled()) 
			logger.trace("RETURN parseAttributes(), #attr=" + 
					attributes.size());
		return attributes;
	}

	private Document parseFile(String schemaFile) 
	throws MetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER parseFile() with filename " + schemaFile);
		Document document;
		try {
			DocumentBuilderFactory factory = 
				DocumentBuilderFactory.newInstance();
			DocumentBuilder builder;
			builder = factory.newDocumentBuilder();
			document = builder.parse(schemaFile);
		} catch (ParserConfigurationException e) {
			String msg = "Unable to configure document parser.";
			logger.warn(msg);
			throw new MetadataException(msg, e);
		} catch (SAXException e) {
			String msg = "Unable to parse schema.";
			logger.warn(msg + e);
			throw new MetadataException(msg, e);
		} catch (IOException e) {
			String msg = "Unable to access schema file " + 
			schemaFile + ".";
			logger.warn(msg);
			throw new MetadataException(msg, e);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN parseFile()");
		return document;
	}

	/**
	 * Retrieve the metadata about a particular extent
	 * 
	 * @param name Name of the extent
	 * @return Metadata about the extent
	 * @throws ExtentDoesNotExistException The given name does not exist
	 */
	public ExtentMetadata getExtentMetadata(String name)
	throws ExtentDoesNotExistException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getExtentMetadata() with name " + name);
		ExtentMetadata schemaMetadata;
		if (_schema.containsKey(name.toLowerCase())) {
			schemaMetadata = _schema.get(name.toLowerCase());
		} else {
			String msg = "Extent " + name + " not found in Schema";
			logger.warn(msg);
			throw new ExtentDoesNotExistException(msg);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN getExtentMetadata()");
		}
		return schemaMetadata;
	}

	public List<ExtentMetadata> getAcquireExtents() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getAcquireExtents()");
		List<ExtentMetadata> result = getExtentsOfType(ExtentType.SENSED);
		if (logger.isDebugEnabled())
			logger.debug("RETURN getAcquireExtents() " +
					"number of sensed sources: " + result.size());
		return result;
	}

	public List<ExtentMetadata> getPushedExtents() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getPushedExtents()");
		List<ExtentMetadata> result = getExtentsOfType(ExtentType.PUSHED);
		if (logger.isDebugEnabled())
			logger.debug("RETURN getPushedExtents() " +
					"number of pushed sources: " + result.size());
		return result;
	}

	public List<ExtentMetadata> getStoredExtents() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getStoredExtents()");
		List<ExtentMetadata> result = getExtentsOfType(ExtentType.TABLE);
		if (logger.isDebugEnabled())
			logger.debug("RETURN getStoredExtents() " +
					"number of stored sources: " + result.size());
		return result;
	}

	private List<ExtentMetadata> getExtentsOfType(ExtentType extentType) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getExtentsOfType() " + extentType);
		List<ExtentMetadata> result = new ArrayList<ExtentMetadata>(); 
		Set<String> extentNames = _schema.keySet();
		for (String name : extentNames) {
			ExtentMetadata schema = _schema.get(name);
			if (schema.getExtentType() == extentType) 
				result.add(schema);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN getExtentsOfType() " +
					"number of extents: " + result.size());
		return result;
	}

	public Types getTypes() {
		return _types;
	}

	/**
	 * Return the names of the available extents
	 * @return
	 */
	public Collection<String> getExtentNames() {
		return _schema.keySet();
	}


	/**
	 * Updates metadata for each extent with cardinality estimations.
	 * For sensor networks, this depends on the number of sites contributing
	 * data to the extent.  For push-based streams, this depends on the 
	 * average rate at which tuples are produced.  For relations, this depends
	 * on the size of the relation.  Note that this is currently only correctly 
	 * implemented for sensor networks.  Note also that this method would support
	 * having a extent with different sources as input, a feature that probably wont
	 * be implemented.
	 * @throws ExtentDoesNotExistException
	 */
	private void doCardinalityEstimations() 
	throws ExtentDoesNotExistException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER doCardinalityEstimations()");
		for (String extentName : this.getExtentNames()) {
			ExtentMetadata em = this.getExtentMetadata(extentName);
			int cardinality = 0;
			for (SourceMetadataAbstract sm : _sources.getSources()) {
				if (sm.getSourceType()==SourceType.SENSOR_NETWORK) {
					SensorNetworkSourceMetadata snsm = 
						(SensorNetworkSourceMetadata)sm;
					int[] sites = snsm.getSourceSites();
					cardinality += sites.length;
					em.setCardinality(cardinality);
				} else {
					//TODO: Cardinality estimates for non-sensor network sources
					//should be reviewed, if they matter.
					cardinality++;
				}
			}
		//This causes test testPullStreamServiceSource to fail. ask alasdair about this.
		//			em.setCardinality(cardinality);
		}
		if (logger.isTraceEnabled())
			logger.trace("RETURN doCardinalityEstimations()");
	}
	
	public CostParameters getCostParameters() {
		return this.costParams;
	}

	/**
	 * Retrieve the sources associated with the specified extent
	 * 
	 * @param extentName name of the extent to discover sources for
	 * @return the list of sources for the given extent
	 */
	public List<SourceMetadataAbstract> getSources(String extentName) {
		return _sources.getSources(extentName);
	}

	public void addDataSource(String string, String url,
			SourceType sourceType) 
	throws MalformedURLException, SNEEDataSourceException, 
	SchemaMetadataException, TypeMappingException, SourceMetadataException 
	{
		_sources.addServiceSource(string, url, sourceType);
	}
	
}