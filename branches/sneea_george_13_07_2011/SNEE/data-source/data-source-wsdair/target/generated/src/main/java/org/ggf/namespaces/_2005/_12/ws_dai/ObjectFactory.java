
package org.ggf.namespaces._2005._12.ws_dai;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.bind.annotation.adapters.CollapsedStringAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import javax.xml.namespace.QName;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the org.ggf.namespaces._2005._12.ws_dai package. 
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

    private final static QName _LanguageMap_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "LanguageMap");
    private final static QName _DataResourceAddress_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "DataResourceAddress");
    private final static QName _TransactionInitiation_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "TransactionInitiation");
    private final static QName _TransactionIsolation_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "TransactionIsolation");
    private final static QName _InvalidExpressionFault_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "InvalidExpressionFault");
    private final static QName _DatasetFormatURI_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "DatasetFormatURI");
    private final static QName _DataResourceManagement_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "DataResourceManagement");
    private final static QName _DataResourceUnavailableFault_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "DataResourceUnavailableFault");
    private final static QName _InvalidDatasetFormatFault_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "InvalidDatasetFormatFault");
    private final static QName _ParentSensitiveToChild_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "ParentSensitiveToChild");
    private final static QName _DatasetMap_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "DatasetMap");
    private final static QName _ParentDataResource_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "ParentDataResource");
    private final static QName _FactoryRequest_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "FactoryRequest");
    private final static QName _ConfigurationDocumentQName_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "ConfigurationDocumentQName");
    private final static QName _InvalidLanguageFault_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "InvalidLanguageFault");
    private final static QName _DatasetData_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "DatasetData");
    private final static QName _Readable_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "Readable");
    private final static QName _DataResourceAddressList_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "DataResourceAddressList");
    private final static QName _InvalidResourceNameFault_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "InvalidResourceNameFault");
    private final static QName _ServiceBusyFault_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "ServiceBusyFault");
    private final static QName _ConfigurationMap_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "ConfigurationMap");
    private final static QName _NotAuthorizedFault_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "NotAuthorizedFault");
    private final static QName _Request_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "Request");
    private final static QName _LanguageURI_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "LanguageURI");
    private final static QName _ConfigurationDocument_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "ConfigurationDocument");
    private final static QName _DataResourceAbstractName_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "DataResourceAbstractName");
    private final static QName _PortTypeQName_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "PortTypeQName");
    private final static QName _Dataset_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "Dataset");
    private final static QName _MessageQName_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "MessageQName");
    private final static QName _PropertyDocument_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "PropertyDocument");
    private final static QName _ConcurrentAccess_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "ConcurrentAccess");
    private final static QName _ChildSensitiveToParent_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "ChildSensitiveToParent");
    private final static QName _InvalidPortTypeQNameFault_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "InvalidPortTypeQNameFault");
    private final static QName _InvalidConfigurationDocumentFault_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "InvalidConfigurationDocumentFault");
    private final static QName _Expression_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "Expression");
    private final static QName _Writeable_QNAME = new QName("http://www.ggf.org/namespaces/2005/12/WS-DAI", "Writeable");

    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: org.ggf.namespaces._2005._12.ws_dai
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link NotAuthorizedFaultType }
     * 
     */
    public NotAuthorizedFaultType createNotAuthorizedFaultType() {
        return new NotAuthorizedFaultType();
    }

    /**
     * Create an instance of {@link GenericExpression }
     * 
     */
    public GenericExpression createGenericExpression() {
        return new GenericExpression();
    }

    /**
     * Create an instance of {@link FactoryRequestType }
     * 
     */
    public FactoryRequestType createFactoryRequestType() {
        return new FactoryRequestType();
    }

    /**
     * Create an instance of {@link DataResourceDescription }
     * 
     */
    public DataResourceDescription createDataResourceDescription() {
        return new DataResourceDescription();
    }

    /**
     * Create an instance of {@link InvalidConfigurationDocumentFaultType }
     * 
     */
    public InvalidConfigurationDocumentFaultType createInvalidConfigurationDocumentFaultType() {
        return new InvalidConfigurationDocumentFaultType();
    }

    /**
     * Create an instance of {@link DatasetType }
     * 
     */
    public DatasetType createDatasetType() {
        return new DatasetType();
    }

    /**
     * Create an instance of {@link GetResourceListRequest }
     * 
     */
    public GetResourceListRequest createGetResourceListRequest() {
        return new GetResourceListRequest();
    }

    /**
     * Create an instance of {@link GenericQueryResponse }
     * 
     */
    public GenericQueryResponse createGenericQueryResponse() {
        return new GenericQueryResponse();
    }

    /**
     * Create an instance of {@link ResolveResponse }
     * 
     */
    public ResolveResponse createResolveResponse() {
        return new ResolveResponse();
    }

    /**
     * Create an instance of {@link ResolveRequest }
     * 
     */
    public ResolveRequest createResolveRequest() {
        return new ResolveRequest();
    }

    /**
     * Create an instance of {@link ExpressionType }
     * 
     */
    public ExpressionType createExpressionType() {
        return new ExpressionType();
    }

    /**
     * Create an instance of {@link GetDataResourcePropertyDocumentRequest }
     * 
     */
    public GetDataResourcePropertyDocumentRequest createGetDataResourcePropertyDocumentRequest() {
        return new GetDataResourcePropertyDocumentRequest();
    }

    /**
     * Create an instance of {@link ConfigurationMapType.DefaultConfigurationDocument }
     * 
     */
    public ConfigurationMapType.DefaultConfigurationDocument createConfigurationMapTypeDefaultConfigurationDocument() {
        return new ConfigurationMapType.DefaultConfigurationDocument();
    }

    /**
     * Create an instance of {@link DestroyDataResourceRequest }
     * 
     */
    public DestroyDataResourceRequest createDestroyDataResourceRequest() {
        return new DestroyDataResourceRequest();
    }

    /**
     * Create an instance of {@link GenericQueryRequest }
     * 
     */
    public GenericQueryRequest createGenericQueryRequest() {
        return new GenericQueryRequest();
    }

    /**
     * Create an instance of {@link DatasetMapType }
     * 
     */
    public DatasetMapType createDatasetMapType() {
        return new DatasetMapType();
    }

    /**
     * Create an instance of {@link InvalidLanguageFaultType }
     * 
     */
    public InvalidLanguageFaultType createInvalidLanguageFaultType() {
        return new InvalidLanguageFaultType();
    }

    /**
     * Create an instance of {@link DataResourceAddressListType }
     * 
     */
    public DataResourceAddressListType createDataResourceAddressListType() {
        return new DataResourceAddressListType();
    }

    /**
     * Create an instance of {@link InvalidPortTypeQNameFaultType }
     * 
     */
    public InvalidPortTypeQNameFaultType createInvalidPortTypeQNameFaultType() {
        return new InvalidPortTypeQNameFaultType();
    }

    /**
     * Create an instance of {@link InvalidResourceNameFaultType }
     * 
     */
    public InvalidResourceNameFaultType createInvalidResourceNameFaultType() {
        return new InvalidResourceNameFaultType();
    }

    /**
     * Create an instance of {@link BaseRequestType }
     * 
     */
    public BaseRequestType createBaseRequestType() {
        return new BaseRequestType();
    }

    /**
     * Create an instance of {@link RequestType }
     * 
     */
    public RequestType createRequestType() {
        return new RequestType();
    }

    /**
     * Create an instance of {@link LanguageMapType }
     * 
     */
    public LanguageMapType createLanguageMapType() {
        return new LanguageMapType();
    }

    /**
     * Create an instance of {@link InvalidDatasetFormatFaultType }
     * 
     */
    public InvalidDatasetFormatFaultType createInvalidDatasetFormatFaultType() {
        return new InvalidDatasetFormatFaultType();
    }

    /**
     * Create an instance of {@link InvalidExpressionFaultType }
     * 
     */
    public InvalidExpressionFaultType createInvalidExpressionFaultType() {
        return new InvalidExpressionFaultType();
    }

    /**
     * Create an instance of {@link DestroyDataResourceResponse }
     * 
     */
    public DestroyDataResourceResponse createDestroyDataResourceResponse() {
        return new DestroyDataResourceResponse();
    }

    /**
     * Create an instance of {@link DataResourceUnavailableFaultType }
     * 
     */
    public DataResourceUnavailableFaultType createDataResourceUnavailableFaultType() {
        return new DataResourceUnavailableFaultType();
    }

    /**
     * Create an instance of {@link DatasetDataType }
     * 
     */
    public DatasetDataType createDatasetDataType() {
        return new DatasetDataType();
    }

    /**
     * Create an instance of {@link ServiceBusyFaultType }
     * 
     */
    public ServiceBusyFaultType createServiceBusyFaultType() {
        return new ServiceBusyFaultType();
    }

    /**
     * Create an instance of {@link PropertyDocumentType }
     * 
     */
    public PropertyDocumentType createPropertyDocumentType() {
        return new PropertyDocumentType();
    }

    /**
     * Create an instance of {@link GetResourceListResponse }
     * 
     */
    public GetResourceListResponse createGetResourceListResponse() {
        return new GetResourceListResponse();
    }

    /**
     * Create an instance of {@link DataResourceAddressType }
     * 
     */
    public DataResourceAddressType createDataResourceAddressType() {
        return new DataResourceAddressType();
    }

    /**
     * Create an instance of {@link ConfigurationDocumentType }
     * 
     */
    public ConfigurationDocumentType createConfigurationDocumentType() {
        return new ConfigurationDocumentType();
    }

    /**
     * Create an instance of {@link ConfigurationMapType }
     * 
     */
    public ConfigurationMapType createConfigurationMapType() {
        return new ConfigurationMapType();
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link LanguageMapType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "LanguageMap")
    public JAXBElement<LanguageMapType> createLanguageMap(LanguageMapType value) {
        return new JAXBElement<LanguageMapType>(_LanguageMap_QNAME, LanguageMapType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link DataResourceAddressType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "DataResourceAddress")
    public JAXBElement<DataResourceAddressType> createDataResourceAddress(DataResourceAddressType value) {
        return new JAXBElement<DataResourceAddressType>(_DataResourceAddress_QNAME, DataResourceAddressType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "TransactionInitiation")
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    public JAXBElement<String> createTransactionInitiation(String value) {
        return new JAXBElement<String>(_TransactionInitiation_QNAME, String.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "TransactionIsolation")
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    public JAXBElement<String> createTransactionIsolation(String value) {
        return new JAXBElement<String>(_TransactionIsolation_QNAME, String.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link InvalidExpressionFaultType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "InvalidExpressionFault")
    public JAXBElement<InvalidExpressionFaultType> createInvalidExpressionFault(InvalidExpressionFaultType value) {
        return new JAXBElement<InvalidExpressionFaultType>(_InvalidExpressionFault_QNAME, InvalidExpressionFaultType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "DatasetFormatURI")
    public JAXBElement<String> createDatasetFormatURI(String value) {
        return new JAXBElement<String>(_DatasetFormatURI_QNAME, String.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "DataResourceManagement")
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    public JAXBElement<String> createDataResourceManagement(String value) {
        return new JAXBElement<String>(_DataResourceManagement_QNAME, String.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link DataResourceUnavailableFaultType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "DataResourceUnavailableFault")
    public JAXBElement<DataResourceUnavailableFaultType> createDataResourceUnavailableFault(DataResourceUnavailableFaultType value) {
        return new JAXBElement<DataResourceUnavailableFaultType>(_DataResourceUnavailableFault_QNAME, DataResourceUnavailableFaultType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link InvalidDatasetFormatFaultType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "InvalidDatasetFormatFault")
    public JAXBElement<InvalidDatasetFormatFaultType> createInvalidDatasetFormatFault(InvalidDatasetFormatFaultType value) {
        return new JAXBElement<InvalidDatasetFormatFaultType>(_InvalidDatasetFormatFault_QNAME, InvalidDatasetFormatFaultType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "ParentSensitiveToChild")
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    public JAXBElement<String> createParentSensitiveToChild(String value) {
        return new JAXBElement<String>(_ParentSensitiveToChild_QNAME, String.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link DatasetMapType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "DatasetMap")
    public JAXBElement<DatasetMapType> createDatasetMap(DatasetMapType value) {
        return new JAXBElement<DatasetMapType>(_DatasetMap_QNAME, DatasetMapType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link DataResourceAddressType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "ParentDataResource")
    public JAXBElement<DataResourceAddressType> createParentDataResource(DataResourceAddressType value) {
        return new JAXBElement<DataResourceAddressType>(_ParentDataResource_QNAME, DataResourceAddressType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link FactoryRequestType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "FactoryRequest")
    public JAXBElement<FactoryRequestType> createFactoryRequest(FactoryRequestType value) {
        return new JAXBElement<FactoryRequestType>(_FactoryRequest_QNAME, FactoryRequestType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link QName }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "ConfigurationDocumentQName")
    public JAXBElement<QName> createConfigurationDocumentQName(QName value) {
        return new JAXBElement<QName>(_ConfigurationDocumentQName_QNAME, QName.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link InvalidLanguageFaultType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "InvalidLanguageFault")
    public JAXBElement<InvalidLanguageFaultType> createInvalidLanguageFault(InvalidLanguageFaultType value) {
        return new JAXBElement<InvalidLanguageFaultType>(_InvalidLanguageFault_QNAME, InvalidLanguageFaultType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link DatasetDataType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "DatasetData")
    public JAXBElement<DatasetDataType> createDatasetData(DatasetDataType value) {
        return new JAXBElement<DatasetDataType>(_DatasetData_QNAME, DatasetDataType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link Boolean }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "Readable", defaultValue = "true")
    public JAXBElement<Boolean> createReadable(Boolean value) {
        return new JAXBElement<Boolean>(_Readable_QNAME, Boolean.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link DataResourceAddressListType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "DataResourceAddressList")
    public JAXBElement<DataResourceAddressListType> createDataResourceAddressList(DataResourceAddressListType value) {
        return new JAXBElement<DataResourceAddressListType>(_DataResourceAddressList_QNAME, DataResourceAddressListType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link InvalidResourceNameFaultType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "InvalidResourceNameFault")
    public JAXBElement<InvalidResourceNameFaultType> createInvalidResourceNameFault(InvalidResourceNameFaultType value) {
        return new JAXBElement<InvalidResourceNameFaultType>(_InvalidResourceNameFault_QNAME, InvalidResourceNameFaultType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link ServiceBusyFaultType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "ServiceBusyFault")
    public JAXBElement<ServiceBusyFaultType> createServiceBusyFault(ServiceBusyFaultType value) {
        return new JAXBElement<ServiceBusyFaultType>(_ServiceBusyFault_QNAME, ServiceBusyFaultType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link ConfigurationMapType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "ConfigurationMap")
    public JAXBElement<ConfigurationMapType> createConfigurationMap(ConfigurationMapType value) {
        return new JAXBElement<ConfigurationMapType>(_ConfigurationMap_QNAME, ConfigurationMapType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link NotAuthorizedFaultType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "NotAuthorizedFault")
    public JAXBElement<NotAuthorizedFaultType> createNotAuthorizedFault(NotAuthorizedFaultType value) {
        return new JAXBElement<NotAuthorizedFaultType>(_NotAuthorizedFault_QNAME, NotAuthorizedFaultType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link RequestType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "Request")
    public JAXBElement<RequestType> createRequest(RequestType value) {
        return new JAXBElement<RequestType>(_Request_QNAME, RequestType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "LanguageURI")
    public JAXBElement<String> createLanguageURI(String value) {
        return new JAXBElement<String>(_LanguageURI_QNAME, String.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link ConfigurationDocumentType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "ConfigurationDocument")
    public JAXBElement<ConfigurationDocumentType> createConfigurationDocument(ConfigurationDocumentType value) {
        return new JAXBElement<ConfigurationDocumentType>(_ConfigurationDocument_QNAME, ConfigurationDocumentType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "DataResourceAbstractName")
    public JAXBElement<String> createDataResourceAbstractName(String value) {
        return new JAXBElement<String>(_DataResourceAbstractName_QNAME, String.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link QName }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "PortTypeQName")
    public JAXBElement<QName> createPortTypeQName(QName value) {
        return new JAXBElement<QName>(_PortTypeQName_QNAME, QName.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link DatasetType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "Dataset")
    public JAXBElement<DatasetType> createDataset(DatasetType value) {
        return new JAXBElement<DatasetType>(_Dataset_QNAME, DatasetType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link QName }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "MessageQName")
    public JAXBElement<QName> createMessageQName(QName value) {
        return new JAXBElement<QName>(_MessageQName_QNAME, QName.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link PropertyDocumentType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "PropertyDocument")
    public JAXBElement<PropertyDocumentType> createPropertyDocument(PropertyDocumentType value) {
        return new JAXBElement<PropertyDocumentType>(_PropertyDocument_QNAME, PropertyDocumentType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link Boolean }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "ConcurrentAccess")
    public JAXBElement<Boolean> createConcurrentAccess(Boolean value) {
        return new JAXBElement<Boolean>(_ConcurrentAccess_QNAME, Boolean.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "ChildSensitiveToParent")
    @XmlJavaTypeAdapter(CollapsedStringAdapter.class)
    public JAXBElement<String> createChildSensitiveToParent(String value) {
        return new JAXBElement<String>(_ChildSensitiveToParent_QNAME, String.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link InvalidPortTypeQNameFaultType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "InvalidPortTypeQNameFault")
    public JAXBElement<InvalidPortTypeQNameFaultType> createInvalidPortTypeQNameFault(InvalidPortTypeQNameFaultType value) {
        return new JAXBElement<InvalidPortTypeQNameFaultType>(_InvalidPortTypeQNameFault_QNAME, InvalidPortTypeQNameFaultType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link InvalidConfigurationDocumentFaultType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "InvalidConfigurationDocumentFault")
    public JAXBElement<InvalidConfigurationDocumentFaultType> createInvalidConfigurationDocumentFault(InvalidConfigurationDocumentFaultType value) {
        return new JAXBElement<InvalidConfigurationDocumentFaultType>(_InvalidConfigurationDocumentFault_QNAME, InvalidConfigurationDocumentFaultType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link ExpressionType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "Expression")
    public JAXBElement<ExpressionType> createExpression(ExpressionType value) {
        return new JAXBElement<ExpressionType>(_Expression_QNAME, ExpressionType.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link Boolean }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://www.ggf.org/namespaces/2005/12/WS-DAI", name = "Writeable")
    public JAXBElement<Boolean> createWriteable(Boolean value) {
        return new JAXBElement<Boolean>(_Writeable_QNAME, Boolean.class, null, value);
    }

}
