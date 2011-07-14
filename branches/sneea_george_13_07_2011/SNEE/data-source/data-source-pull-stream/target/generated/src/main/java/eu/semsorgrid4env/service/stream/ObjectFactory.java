
package eu.semsorgrid4env.service.stream;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.namespace.QName;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the eu.semsorgrid4env.service.stream package. 
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

    private final static QName _DurationOrTuples_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS", "DurationOrTuples");
    private final static QName _StreamDescription_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS", "StreamDescription");
    private final static QName _AbsoluteOrRelativeTime_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS", "AbsoluteOrRelativeTime");
    private final static QName _TimeOrSequenceNumber_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS", "TimeOrSequenceNumber");
    private final static QName _StreamRate_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS", "StreamRate");
    private final static QName _StreamItem_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS", "StreamItem");
    private final static QName _StreamPropertyDocument_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS", "StreamPropertyDocument");

    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: eu.semsorgrid4env.service.stream
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link StreamDescriptionType }
     * 
     */
    public StreamDescriptionType createStreamDescriptionType() {
        return new StreamDescriptionType();
    }

    /**
     * Create an instance of {@link StreamPropertyDocumentType }
     * 
     */
    public StreamPropertyDocumentType createStreamPropertyDocumentType() {
        return new StreamPropertyDocumentType();
    }

    /**
     * Create an instance of {@link StreamItemType }
     * 
     */
    public StreamItemType createStreamItemType() {
        return new StreamItemType();
    }

    /**
     * Create an instance of {@link StreamRateType }
     * 
     */
    public StreamRateType createStreamRateType() {
        return new StreamRateType();
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS", name = "DurationOrTuples")
    public JAXBElement<String> createDurationOrTuples(String value) {
        return new JAXBElement<String>(_DurationOrTuples_QNAME, String.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link StreamDescriptionType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS", name = "StreamDescription")
    public JAXBElement<StreamDescriptionType> createStreamDescription(StreamDescriptionType value) {
        return new JAXBElement<StreamDescriptionType>(_StreamDescription_QNAME, StreamDescriptionType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS", name = "AbsoluteOrRelativeTime")
    public JAXBElement<String> createAbsoluteOrRelativeTime(String value) {
        return new JAXBElement<String>(_AbsoluteOrRelativeTime_QNAME, String.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS", name = "TimeOrSequenceNumber")
    public JAXBElement<String> createTimeOrSequenceNumber(String value) {
        return new JAXBElement<String>(_TimeOrSequenceNumber_QNAME, String.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link StreamRateType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS", name = "StreamRate")
    public JAXBElement<StreamRateType> createStreamRate(StreamRateType value) {
        return new JAXBElement<StreamRateType>(_StreamRate_QNAME, StreamRateType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link StreamItemType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS", name = "StreamItem")
    public JAXBElement<StreamItemType> createStreamItem(StreamItemType value) {
        return new JAXBElement<StreamItemType>(_StreamItem_QNAME, StreamItemType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link StreamPropertyDocumentType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS", name = "StreamPropertyDocument")
    public JAXBElement<StreamPropertyDocumentType> createStreamPropertyDocument(StreamPropertyDocumentType value) {
        return new JAXBElement<StreamPropertyDocumentType>(_StreamPropertyDocument_QNAME, StreamPropertyDocumentType.class, null, value);
    }

}
