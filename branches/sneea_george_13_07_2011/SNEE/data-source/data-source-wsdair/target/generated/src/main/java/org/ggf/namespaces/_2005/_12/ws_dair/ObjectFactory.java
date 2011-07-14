
package org.ggf.namespaces._2005._12.ws_dair;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.bind.annotation.adapters.CollapsedStringAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import javax.xml.namespace.QName;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the org.ggf.namespaces._2005._12.ws_dair package. 
 * <p>An ObjectFactory allows you to programatically 
 * construct new instances of the Java representation 
 * for XML content. The Java representation of XML 
 * content can consist of schema derived interfaces 
 * and classes representing the binding of schema 
 * type definitions, element declarations and model 
 * groups.  Factory methods for each of these are 
 * provided in this class.
 * 
 */
@XmlRegistry
public class ObjectFactory {

    private final static QName _InvalidCountFault_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAIR", "InvalidCountFault");
    private final static QName _SQLOutputParameter_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAIR", "SQLOutputParameter");
    private final static QName _InvalidGetTuplesRequestFault_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAIR", "InvalidGetTuplesRequestFault");
    private final static QName _AccessMode_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAIR", "AccessMode");
    private final static QName _SQLUpdateCount_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAIR", "SQLUpdateCount");
    private final static QName _RowSchema_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAIR", "RowSchema");
    private final static QName _NoOfRows_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAIR", "NoOfRows");
    private final static QName _SQLCommunicationsArea_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAIR", "SQLCommunicationsArea");
    private final static QName _SQLDataset_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAIR", "SQLDataset");
    private final static QName _InvalidPositionFault_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAIR", "InvalidPositionFault");
    private final static QName _SQLRowsetPropertyDocument_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAIR", "SQLRowsetPropertyDocument");
    private final static QName _SQLReturnValue_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAIR", "SQLReturnValue");
    private final static QName _SQLPropertyDocument_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAIR", "SQLPropertyDocument");
    private final static QName _SQLRowsetConfigurationDocument_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAIR", "SQLRowsetConfigurationDocument");

    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: org.ggf.namespaces._2005._12.ws_dair
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link SQLDatasetType }
     * 
     */
    public SQLDatasetType createSQLDatasetType() {
        return new SQLDatasetType();
    }

    /**
     * Create an instance of {@link RowSchemaType }
     * 
     */
    public RowSchemaType createRowSchemaType() {
        return new RowSchemaType();
    }

    /**
     * Create an instance of {@link InvalidGetTuplesRequestFaultType }
     * 
     */
    public InvalidGetTuplesRequestFaultType createInvalidGetTuplesRequestFaultType() {
        return new InvalidGetTuplesRequestFaultType();
    }

    /**
     * Create an instance of {@link InvalidCountFaultType }
     * 
     */
    public InvalidCountFaultType createInvalidCountFaultType() {
        return new InvalidCountFaultType();
    }

    /**
     * Create an instance of {@link SQLCommunicationsAreaType }
     * 
     */
    public SQLCommunicationsAreaType createSQLCommunicationsAreaType() {
        return new SQLCommunicationsAreaType();
    }

    /**
     * Create an instance of {@link GetTuplesResponse }
     * 
     */
    public GetTuplesResponse createGetTuplesResponse() {
        return new GetTuplesResponse();
    }

    /**
     * Create an instance of {@link InvalidPositionFaultType }
     * 
     */
    public InvalidPositionFaultType createInvalidPositionFaultType() {
        return new InvalidPositionFaultType();
    }

    /**
     * Create an instance of {@link SQLRowsetPropertyDocumentType }
     * 
     */
    public SQLRowsetPropertyDocumentType createSQLRowsetPropertyDocumentType() {
        return new SQLRowsetPropertyDocumentType();
    }

    /**
     * Create an instance of {@link SQLRowsetConfigurationDocumentType }
     * 
     */
    public SQLRowsetConfigurationDocumentType createSQLRowsetConfigurationDocumentType() {
        return new SQLRowsetConfigurationDocumentType();
    }

    /**
     * Create an instance of {@link SQLPropertyDocumentType }
     * 
     */
    public SQLPropertyDocumentType createSQLPropertyDocumentType() {
        return new SQLPropertyDocumentType();
    }

    /**
     * Create an instance of {@link SchemaDescription }
     * 
     */
    public SchemaDescription createSchemaDescription() {
        return new SchemaDescription();
    }

    /**
     * Create an instance of {@link GetTuplesRequest }
     * 
     */
    public GetTuplesRequest createGetTuplesRequest() {
        return new GetTuplesRequest();
    }

    /**
     * Create an instance of {@link SQLOutputParameterType }
     * 
     */
    public SQLOutputParameterType createSQLOutputParameterType() {
        return new SQLOutputParameterType();
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link InvalidCountFaultType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAIR", name = "InvalidCountFault")
    public JAXBElement<InvalidCountFaultType> createInvalidCountFault(InvalidCountFaultType value) {
        return new JAXBElement<InvalidCountFaultType>(_InvalidCountFault_QNAME, InvalidCountFaultType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link SQLOutputParameterType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAIR", name = "SQLOutputParameter")
    public JAXBElement<SQLOutputParameterType> createSQLOutputParameter(SQLOutputParameterType value) {
        return new JAXBElement<SQLOutputParameterType>(_SQLOutputParameter_QNAME, SQLOutputParameterType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link InvalidGetTuplesRequestFaultType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAIR", name = "InvalidGetTuplesRequestFault")
    public JAXBElement<InvalidGetTuplesRequestFaultType> createInvalidGetTuplesRequestFault(InvalidGetTuplesRequestFaultType value) {
        return new JAXBElement<InvalidGetTuplesRequestFaultType>(_InvalidGetTuplesRequestFault_QNAME, InvalidGetTuplesRequestFaultType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAIR", name = "AccessMode")
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    public JAXBElement<String> createAccessMode(String value) {
        return new JAXBElement<String>(_AccessMode_QNAME, String.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link Integer }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAIR", name = "SQLUpdateCount")
    public JAXBElement<Integer> createSQLUpdateCount(Integer value) {
        return new JAXBElement<Integer>(_SQLUpdateCount_QNAME, Integer.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link RowSchemaType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAIR", name = "RowSchema")
    public JAXBElement<RowSchemaType> createRowSchema(RowSchemaType value) {
        return new JAXBElement<RowSchemaType>(_RowSchema_QNAME, RowSchemaType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link Integer }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAIR", name = "NoOfRows")
    public JAXBElement<Integer> createNoOfRows(Integer value) {
        return new JAXBElement<Integer>(_NoOfRows_QNAME, Integer.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link SQLCommunicationsAreaType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAIR", name = "SQLCommunicationsArea")
    public JAXBElement<SQLCommunicationsAreaType> createSQLCommunicationsArea(SQLCommunicationsAreaType value) {
        return new JAXBElement<SQLCommunicationsAreaType>(_SQLCommunicationsArea_QNAME, SQLCommunicationsAreaType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link SQLDatasetType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAIR", name = "SQLDataset", substitutionHeadNamespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", substitutionHeadName = "Dataset")
    public JAXBElement<SQLDatasetType> createSQLDataset(SQLDatasetType value) {
        return new JAXBElement<SQLDatasetType>(_SQLDataset_QNAME, SQLDatasetType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link InvalidPositionFaultType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAIR", name = "InvalidPositionFault")
    public JAXBElement<InvalidPositionFaultType> createInvalidPositionFault(InvalidPositionFaultType value) {
        return new JAXBElement<InvalidPositionFaultType>(_InvalidPositionFault_QNAME, InvalidPositionFaultType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link SQLRowsetPropertyDocumentType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAIR", name = "SQLRowsetPropertyDocument")
    public JAXBElement<SQLRowsetPropertyDocumentType> createSQLRowsetPropertyDocument(SQLRowsetPropertyDocumentType value) {
        return new JAXBElement<SQLRowsetPropertyDocumentType>(_SQLRowsetPropertyDocument_QNAME, SQLRowsetPropertyDocumentType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAIR", name = "SQLReturnValue")
    public JAXBElement<String> createSQLReturnValue(String value) {
        return new JAXBElement<String>(_SQLReturnValue_QNAME, String.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link SQLPropertyDocumentType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAIR", name = "SQLPropertyDocument")
    public JAXBElement<SQLPropertyDocumentType> createSQLPropertyDocument(SQLPropertyDocumentType value) {
        return new JAXBElement<SQLPropertyDocumentType>(_SQLPropertyDocument_QNAME, SQLPropertyDocumentType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link SQLRowsetConfigurationDocumentType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAIR", name = "SQLRowsetConfigurationDocument", substitutionHeadNamespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", substitutionHeadName = "ConfigurationDocument")
    public JAXBElement<SQLRowsetConfigurationDocumentType> createSQLRowsetConfigurationDocument(SQLRowsetConfigurationDocumentType value) {
        return new JAXBElement<SQLRowsetConfigurationDocumentType>(_SQLRowsetConfigurationDocument_QNAME, SQLRowsetConfigurationDocumentType.class, null, value);
    }

}
