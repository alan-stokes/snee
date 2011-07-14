
package eu.semsorgrid4env.service.stream.push;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.datatype.Duration;
import javax.xml.namespace.QName;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the eu.semsorgrid4env.service.stream.push package. 
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

    private final static QName _PublicationPolicy_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Push", "PublicationPolicy");
    private final static QName _SupportRawMessages_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Push", "SupportRawMessages");
    private final static QName _SubscriptionPolicy_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Push", "SubscriptionPolicy");
    private final static QName _MaximumSubscriptionPeriod_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Push", "MaximumSubscriptionPeriod");
    private final static QName _MessageContentExpressionLanguageURI_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Push", "MessageContentExpressionLanguageURI");
    private final static QName _MaximumNumberItems_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Push", "MaximumNumberItems");
    private final static QName _FilterLanguageURI_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Push", "FilterLanguageURI");
    private final static QName _PushStreamPropertyDocument_QNAME = new QName("http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Push", "PushStreamPropertyDocument");

    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: eu.semsorgrid4env.service.stream.push
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link SubscriptionPolicyType }
     * 
     */
    public SubscriptionPolicyType createSubscriptionPolicyType() {
        return new SubscriptionPolicyType();
    }

    /**
     * Create an instance of {@link PushStreamPropertyDocumentType }
     * 
     */
    public PushStreamPropertyDocumentType createPushStreamPropertyDocumentType() {
        return new PushStreamPropertyDocumentType();
    }

    /**
     * Create an instance of {@link PublicationPolicyType }
     * 
     */
    public PublicationPolicyType createPublicationPolicyType() {
        return new PublicationPolicyType();
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link PublicationPolicyType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Push", name = "PublicationPolicy")
    public JAXBElement<PublicationPolicyType> createPublicationPolicy(PublicationPolicyType value) {
        return new JAXBElement<PublicationPolicyType>(_PublicationPolicy_QNAME, PublicationPolicyType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link Boolean }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Push", name = "SupportRawMessages", defaultValue = "false")
    public JAXBElement<Boolean> createSupportRawMessages(Boolean value) {
        return new JAXBElement<Boolean>(_SupportRawMessages_QNAME, Boolean.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link SubscriptionPolicyType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Push", name = "SubscriptionPolicy")
    public JAXBElement<SubscriptionPolicyType> createSubscriptionPolicy(SubscriptionPolicyType value) {
        return new JAXBElement<SubscriptionPolicyType>(_SubscriptionPolicy_QNAME, SubscriptionPolicyType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link Duration }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Push", name = "MaximumSubscriptionPeriod")
    public JAXBElement<Duration> createMaximumSubscriptionPeriod(Duration value) {
        return new JAXBElement<Duration>(_MaximumSubscriptionPeriod_QNAME, Duration.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Push", name = "MessageContentExpressionLanguageURI")
    public JAXBElement<String> createMessageContentExpressionLanguageURI(String value) {
        return new JAXBElement<String>(_MessageContentExpressionLanguageURI_QNAME, String.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link Integer }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Push", name = "MaximumNumberItems", defaultValue = "1")
    public JAXBElement<Integer> createMaximumNumberItems(Integer value) {
        return new JAXBElement<Integer>(_MaximumNumberItems_QNAME, Integer.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Push", name = "FilterLanguageURI")
    public JAXBElement<String> createFilterLanguageURI(String value) {
        return new JAXBElement<String>(_FilterLanguageURI_QNAME, String.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link PushStreamPropertyDocumentType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Push", name = "PushStreamPropertyDocument", substitutionHeadNamespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", substitutionHeadName = "PropertyDocument")
    public JAXBElement<PushStreamPropertyDocumentType> createPushStreamPropertyDocument(PushStreamPropertyDocumentType value) {
        return new JAXBElement<PushStreamPropertyDocumentType>(_PushStreamPropertyDocument_QNAME, PushStreamPropertyDocumentType.class, null, value);
    }

}
