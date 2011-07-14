
package eu.semsorgrid4env.service.stream.pull;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.datatype.Duration;
import javax.xml.namespace.QName;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the eu.semsorgrid4env.service.stream.pull package. 
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

    private final static QName _InvalidPositionFault_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", "InvalidPositionFault");
    private final static QName _SequenceNumber_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", "SequenceNumber");
    private final static QName _StreamHistory_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", "StreamHistory");
    private final static QName _InvalidCountFault_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", "InvalidCountFault");
    private final static QName _MaximumHistoryDuration_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", "MaximumHistoryDuration");
    private final static QName _MaximumTuples_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", "MaximumTuples");
    private final static QName _PullStreamPropertyDocument_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", "PullStreamPropertyDocument");
    private final static QName _MaximumHistoryTuples_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", "MaximumHistoryTuples");
    private final static QName _Position_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", "Position");
    private final static QName _MaximumTuplesExceededFault_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", "MaximumTuplesExceededFault");
    private final static QName _Count_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", "Count");

    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: eu.semsorgrid4env.service.stream.pull
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link GetStreamNewestItemRequest }
     * 
     */
    public GetStreamNewestItemRequest createGetStreamNewestItemRequest() {
        return new GetStreamNewestItemRequest();
    }

    /**
     * Create an instance of {@link PullStreamPropertyDocumentType }
     * 
     */
    public PullStreamPropertyDocumentType createPullStreamPropertyDocumentType() {
        return new PullStreamPropertyDocumentType();
    }

    /**
     * Create an instance of {@link MaximumTuplesExceededFaultType }
     * 
     */
    public MaximumTuplesExceededFaultType createMaximumTuplesExceededFaultType() {
        return new MaximumTuplesExceededFaultType();
    }

    /**
     * Create an instance of {@link InvalidPositionFaultType }
     * 
     */
    public InvalidPositionFaultType createInvalidPositionFaultType() {
        return new InvalidPositionFaultType();
    }

    /**
     * Create an instance of {@link StreamHistoryType }
     * 
     */
    public StreamHistoryType createStreamHistoryType() {
        return new StreamHistoryType();
    }

    /**
     * Create an instance of {@link InvalidCountFaultType }
     * 
     */
    public InvalidCountFaultType createInvalidCountFaultType() {
        return new InvalidCountFaultType();
    }

    /**
     * Create an instance of {@link GetStreamItemRequest }
     * 
     */
    public GetStreamItemRequest createGetStreamItemRequest() {
        return new GetStreamItemRequest();
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link InvalidPositionFaultType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", name = "InvalidPositionFault")
    public JAXBElement<InvalidPositionFaultType> createInvalidPositionFault(InvalidPositionFaultType value) {
        return new JAXBElement<InvalidPositionFaultType>(_InvalidPositionFault_QNAME, InvalidPositionFaultType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link Integer }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", name = "SequenceNumber")
    public JAXBElement<Integer> createSequenceNumber(Integer value) {
        return new JAXBElement<Integer>(_SequenceNumber_QNAME, Integer.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link StreamHistoryType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", name = "StreamHistory")
    public JAXBElement<StreamHistoryType> createStreamHistory(StreamHistoryType value) {
        return new JAXBElement<StreamHistoryType>(_StreamHistory_QNAME, StreamHistoryType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link InvalidCountFaultType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", name = "InvalidCountFault")
    public JAXBElement<InvalidCountFaultType> createInvalidCountFault(InvalidCountFaultType value) {
        return new JAXBElement<InvalidCountFaultType>(_InvalidCountFault_QNAME, InvalidCountFaultType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link Duration }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", name = "MaximumHistoryDuration")
    public JAXBElement<Duration> createMaximumHistoryDuration(Duration value) {
        return new JAXBElement<Duration>(_MaximumHistoryDuration_QNAME, Duration.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link Integer }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", name = "MaximumTuples")
    public JAXBElement<Integer> createMaximumTuples(Integer value) {
        return new JAXBElement<Integer>(_MaximumTuples_QNAME, Integer.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link PullStreamPropertyDocumentType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", name = "PullStreamPropertyDocument", substitutionHeadNamespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", substitutionHeadName = "PropertyDocument")
    public JAXBElement<PullStreamPropertyDocumentType> createPullStreamPropertyDocument(PullStreamPropertyDocumentType value) {
        return new JAXBElement<PullStreamPropertyDocumentType>(_PullStreamPropertyDocument_QNAME, PullStreamPropertyDocumentType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link Integer }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", name = "MaximumHistoryTuples")
    public JAXBElement<Integer> createMaximumHistoryTuples(Integer value) {
        return new JAXBElement<Integer>(_MaximumHistoryTuples_QNAME, Integer.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", name = "Position")
    public JAXBElement<String> createPosition(String value) {
        return new JAXBElement<String>(_Position_QNAME, String.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link MaximumTuplesExceededFaultType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", name = "MaximumTuplesExceededFault")
    public JAXBElement<MaximumTuplesExceededFaultType> createMaximumTuplesExceededFault(MaximumTuplesExceededFaultType value) {
        return new JAXBElement<MaximumTuplesExceededFaultType>(_MaximumTuplesExceededFault_QNAME, MaximumTuplesExceededFaultType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Pull", name = "Count")
    public JAXBElement<String> createCount(String value) {
        return new JAXBElement<String>(_Count_QNAME, String.class, null, value);
    }

}
