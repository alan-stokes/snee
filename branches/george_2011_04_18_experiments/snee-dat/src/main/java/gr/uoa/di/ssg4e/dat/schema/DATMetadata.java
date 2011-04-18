package gr.uoa.di.ssg4e.dat.schema;

import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;

import gr.uoa.di.ssg4e.dat.DATSubType;
import gr.uoa.di.ssg4e.dat.excep.DATSchemaException;


public class DATMetadata {

	/**
	 * The type and subtype of this metadata. The subType knows internally
	 * the type in which it belongs to. 
	 * */
	private DATSubType _type = null;

	/**
	 * List of the parameters that the DAT requires to function
	 */
	private List<Object> _parameters = new ArrayList<Object>();

	/**
	 * The name of the derived attribute. The name already exists
	 * in the columns of the extent
	 * */
	private String _derivedAttribute = null;

	/**
	 * The source query that is used to receive the data from
	 * */
	private String _src = null;


	private DATMetadata(DATSubType type,
			List<Object> parameters,
			String derivedAttribute,
			String sourceQuery ){
		_type = type;
		_parameters = parameters;
		_derivedAttribute = derivedAttribute;
		_src = sourceQuery;
	}


	/**
	 * Returns the DAT type that the metadata refer to. The type contains
	 * both the type and the subtype of the technique 
	 * */
	public DATSubType getDatType(){
		return _type;
	}

	/** 
	 * Returns the parameters of the associated DAT, which were used to create it
	 *  */
	public List<Object> getParameters(){
		return _parameters;
	}

	/**
	 * Returns the name of the derived attribute
	 * */
	public String getDerivedAttribute(){
		return _derivedAttribute;
	}

	/**
	 * Returns the query that will be used as the source for the DAT (e.g. training,
	 * creating the clusters, identifying densities etc).
	 * */
	public String getSourceQuery(){
		return _src;
	}

	/**
	 * Parses the datParams element that has been given as a parameter.
	 * If the element is null null is returned
	 * 
	 * @return Returns a new instance of a DATMetadata object, where the
	 * the appropriate arguments have been identified. If the initial datParams
	 * parameter is null, then null is returned
	 * 
	 * @throws SchemaMetadataException If the element of datParams does not
	 * contain the appropriate attributes / elements.
	 * */
	public static DATMetadata parse( Element datParams )
	throws DATSchemaException{

		/* If the datParams element is null, then we return null */
		if ( datParams == null )
			return null;

		int paramCnt = 0;
		String type = null;
		String subType = null;
		DATSubType datSubType = null;
		List<Object> paramValues = null;
		String derivedAttr = null;
		String src = null;

		/* The datParams element exists. Parse it */
		try{
			NamedNodeMap attrs = datParams.getAttributes();
			type = attrs.getNamedItem("type").getNodeValue().toString();
			subType = attrs.getNamedItem("subtype").getNodeValue().toString();
			datSubType = DATSubType.getTaggedSubType(subType, type);

			NodeList derAttr = datParams.getElementsByTagName("derivedAttribute");
			attrs = ((Element)derAttr.item(0)).getAttributes();
			derivedAttr = attrs.getNamedItem("name").getNodeValue().toString();

			NodeList params = datParams.getElementsByTagName("parameters");

			/* If there is a parameters attribute, then we parse it. Otherwise, skip */
			if ( params.getLength() != 0 ){

				params = ((Element)params.item(0)).getElementsByTagName("parameter");
				paramCnt = params.getLength();
				paramValues = new ArrayList<Object>(paramCnt);
				for ( int i = 0; i < paramCnt; i++ ){
					Element paramElement = (Element)params.item(i);
					paramValues.add( paramElement.getAttribute("value") );
				}
			}

			NodeList source = datParams.getElementsByTagName("source");
			src = ((Element)source.item(0)).getTextContent();

		}catch(NullPointerException npe ){

			/* In case a null pointer exception occurs, then one of the above elements
			 * did not exist, though it was supposed to */

			if ( type == null ) /* type attribute missing */
				throw new DATSchemaException("Expected attribute type in element datParams but " +
						"was not encountered during parsing. Review your logical-schema.xml file");

			if ( subType == null ) /* subtype attribute missing */
				throw new DATSchemaException("Expected attribute subType in element datParams but " +
						"was not encountered during parsing. Review your logical-schema.xml file");

			if ( derivedAttr == null )  /* derived attribute was missing */
				throw new DATSchemaException("Expected element derivedAttribute in datParams but " +
				"was not encountered during parsing. Review your logical-schema.xml file");

			if ( paramValues == null ) /* parameters element missing */
				throw new DATSchemaException("Expected element parameters in datParams but " +
						"was not encountered during parsing. Review your logical-schema.xml file");
				
			if ( paramCnt != paramValues.size() ) /* attribute value missing in one of parameter element */
				throw new DATSchemaException("Expected attribute value in " + 
						(paramValues.size() + 1) + "-th element parameter but was not encountered. " +
								"Review your logical-schema.xml file");

			if ( src == null ) /* source element missing */
				throw new DATSchemaException("Expected element source in element datParams but " +
						"was not encountered. Review your logical-schema.xml file");
		}

		/* Creates and returns the DAT Metadata attribute */
		return new DATMetadata(datSubType, paramValues, derivedAttr, src);
	}

	/**
	 * Returns the information that a DAT uses in an XML like format.
	 * Exists as a form of interfacing
	 * */
	protected String getAsXML(){
		return null;
	}

}
