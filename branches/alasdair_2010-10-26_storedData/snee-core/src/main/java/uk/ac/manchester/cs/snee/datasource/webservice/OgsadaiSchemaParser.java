package uk.ac.manchester.cs.snee.datasource.webservice;

import java.io.IOException;
import java.io.StringReader;
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
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import uk.ac.manchester.cs.snee.compiler.metadata.schema.AttributeType;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.compiler.metadata.schema.Types;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.Attribute;
import uk.ac.manchester.cs.snee.compiler.queryplan.expressions.DataAttribute;

public class OgsadaiSchemaParser extends SchemaParserAbstract {

	private Logger logger = 
		Logger.getLogger(OgsadaiSchemaParser.class.getName());

	private Map<String, List<Attribute>> extents = 
		new HashMap<String, List<Attribute>>();

	public OgsadaiSchemaParser(String schema, Types types) 
	throws SchemaMetadataException, TypeMappingException {
		super(types);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER OgsadaiSchemaParser() with " +
					schema);
		}
		try {
			StringReader schemaReader = new StringReader(schema);
			
			DocumentBuilderFactory factory = 
				DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document document = 
				builder.parse(new InputSource(schemaReader));
			parse((Element) document.getFirstChild());
		} catch (ParserConfigurationException e) {
			String msg = "Unable to configure document parser.";
			logger.warn(msg);
			throw new SchemaMetadataException(msg, e);
		} catch (SAXException e) {
			String msg = "Unable to parse schema.";
			logger.warn(msg + e);
			throw new SchemaMetadataException(msg, e);
		} catch (IOException e) {
			String msg = "Unknown IOException.";
			logger.warn(msg);
			throw new SchemaMetadataException(msg, e);
		}
		if (logger.isDebugEnabled()) 
			logger.debug("RETURN OgsadaiSchemaParser()");
	}
	
	private void parse(Element root) 
	throws TypeMappingException, SchemaMetadataException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER parse() with " + root);
		}
		NodeList extentNodes = root.getElementsByTagName("table");
		for (int i = 0; i < extentNodes.getLength(); i++) {
			Element element = (Element) extentNodes.item(i);
			String name = element.getAttribute("name").toLowerCase();
			List<Attribute> attributes = parseColumns(element, name);
			extents.put(name, attributes);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN parse()");
		}
	}
	
	private List<Attribute> parseColumns(Element root, String extentName) 
	throws TypeMappingException, SchemaMetadataException {
		if (logger.isTraceEnabled()) {
			logger.trace("ENTER parseColumns() with " + root);
		}
		List<Attribute> attributes = 
			new ArrayList<Attribute>();
		NodeList columns = root.getElementsByTagName("column");	
		for (int i = 0; i < columns.getLength(); i++) {
			Element column = (Element) columns.item(i);
			String columnName = column.getAttribute("name");
			NodeList nameTypeElement = 
				column.getElementsByTagName("sqlJavaTypeID");
			String sqlType = 
				nameTypeElement.item(0).getFirstChild().getNodeValue();
			AttributeType type = inferType(new Integer(sqlType));
			Attribute attr = 
				new DataAttribute(extentName, columnName, type);
			attributes.add(attr);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("RETURN parseColumns() #attr=" +
					attributes.size());
		}
		return attributes;
	}

	public Collection<String> getExtentNames() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getExtentNames()");
		Set<String> extentNames = extents.keySet();
		if (logger.isDebugEnabled())
			logger.debug("RETURN getExtentNames() #names=" +
					extentNames.size());
		return extentNames;
	}
	
	public List<Attribute> getColumns(String extentName) {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getColumns()");
		List<Attribute> attributes = extents.get(extentName);
		if (logger.isDebugEnabled())
			logger.debug("RETURN getColumns(), number of columns " + 
					attributes.size());
		return attributes;
	}

}
