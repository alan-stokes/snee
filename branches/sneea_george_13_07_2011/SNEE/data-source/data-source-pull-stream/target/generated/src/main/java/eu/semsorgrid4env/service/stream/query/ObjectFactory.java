
package eu.semsorgrid4env.service.stream.query;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.namespace.QName;
import eu.semsorgrid4env.service.wsdai.DataResourceAddressListType;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the eu.semsorgrid4env.service.stream.query package. 
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

    private final static QName _StreamingQueryPropertyDocument_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Query", "StreamingQueryPropertyDocument");
    private final static QName _GenericQueryFactoryResponse_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Query", "GenericQueryFactoryResponse");
    private final static QName _StreamQueryExpression_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Query", "StreamQueryExpression");

    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: eu.semsorgrid4env.service.stream.query
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link StreamingQueryPropertyDocumentType }
     * 
     */
    public StreamingQueryPropertyDocumentType createStreamingQueryPropertyDocumentType() {
        return new StreamingQueryPropertyDocumentType();
    }

    /**
     * Create an instance of {@link GenericQueryFactoryRequest }
     * 
     */
    public GenericQueryFactoryRequest createGenericQueryFactoryRequest() {
        return new GenericQueryFactoryRequest();
    }

    /**
     * Create an instance of {@link StreamQueryExpressionType }
     * 
     */
    public StreamQueryExpressionType createStreamQueryExpressionType() {
        return new StreamQueryExpressionType();
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link StreamingQueryPropertyDocumentType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Query", name = "StreamingQueryPropertyDocument", substitutionHeadNamespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", substitutionHeadName = "PropertyDocument")
    public JAXBElement<StreamingQueryPropertyDocumentType> createStreamingQueryPropertyDocument(StreamingQueryPropertyDocumentType value) {
        return new JAXBElement<StreamingQueryPropertyDocumentType>(_StreamingQueryPropertyDocument_QNAME, StreamingQueryPropertyDocumentType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link DataResourceAddressListType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Query", name = "GenericQueryFactoryResponse")
    public JAXBElement<DataResourceAddressListType> createGenericQueryFactoryResponse(DataResourceAddressListType value) {
        return new JAXBElement<DataResourceAddressListType>(_GenericQueryFactoryResponse_QNAME, DataResourceAddressListType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link StreamQueryExpressionType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Query", name = "StreamQueryExpression")
    public JAXBElement<StreamQueryExpressionType> createStreamQueryExpression(StreamQueryExpressionType value) {
        return new JAXBElement<StreamQueryExpressionType>(_StreamQueryExpression_QNAME, StreamQueryExpressionType.class, null, value);
    }

}
