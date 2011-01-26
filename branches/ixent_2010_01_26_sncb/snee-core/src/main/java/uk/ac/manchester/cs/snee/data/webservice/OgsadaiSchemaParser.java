package uk.ac.manchester.cs.snee.data.webservice;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

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

	private Element _root;

	public OgsadaiSchemaParser(String schema, Types types) 
	throws SchemaMetadataException {
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
			_root = (Element) document.getFirstChild();
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
	
	public String getExtentName() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getExtentName()");
		NodeList extents = _root.getElementsByTagName("table");
		Element element = (Element) extents.item(0);
		String name = element.getAttribute("name").toLowerCase();
		if (logger.isDebugEnabled())
			logger.debug("RETURN getExtentName() with " + name);
		return name;
	}
	
	public List<Attribute> getColumns(String extentName) 
	throws TypeMappingException, SchemaMetadataException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getColumns()");
		List<Attribute> attributes = 
			new ArrayList<Attribute>();
		NodeList columns = _root.getElementsByTagName("column");	
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
		if (logger.isDebugEnabled())
			logger.debug("RETURN getColumns(), number of columns " + attributes.size());
		return attributes;
	}

}
