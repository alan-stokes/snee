
package com.sun.java.xml.ns.jdbc;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.namespace.QName;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the com.sun.java.xml.ns.jdbc package. 
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

    private final static QName _UpdateValue_QNAME = new QName("http://java.sun.com/xml/ns/jdbc", "updateValue");
    private final static QName _ColumnValue_QNAME = new QName("http://java.sun.com/xml/ns/jdbc", "columnValue");
    private final static QName _PropertiesMapType_QNAME = new QName("http://java.sun.com/xml/ns/jdbc", "type");
    private final static QName _PropertiesMapClass_QNAME = new QName("http://java.sun.com/xml/ns/jdbc", "class");

    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: com.sun.java.xml.ns.jdbc
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link Data.InsertRow }
     * 
     */
    public Data.InsertRow createDataInsertRow() {
        return new Data.InsertRow();
    }

    /**
     * Create an instance of {@link Metadata.ColumnDefinition }
     * 
     */
    public Metadata.ColumnDefinition createMetadataColumnDefinition() {
        return new Metadata.ColumnDefinition();
    }

    /**
     * Create an instance of {@link Data }
     * 
     */
    public Data createData() {
        return new Data();
    }

    /**
     * Create an instance of {@link Data.CurrentRow }
     * 
     */
    public Data.CurrentRow createDataCurrentRow() {
        return new Data.CurrentRow();
    }

    /**
     * Create an instance of {@link Properties.KeyColumns }
     * 
     */
    public Properties.KeyColumns createPropertiesKeyColumns() {
        return new Properties.KeyColumns();
    }

    /**
     * Create an instance of {@link Metadata }
     * 
     */
    public Metadata createMetadata() {
        return new Metadata();
    }

    /**
     * Create an instance of {@link Properties }
     * 
     */
    public Properties createProperties() {
        return new Properties();
    }

    /**
     * Create an instance of {@link WebRowSet }
     * 
     */
    public WebRowSet createWebRowSet() {
        return new WebRowSet();
    }

    /**
     * Create an instance of {@link Data.ModifyRow }
     * 
     */
    public Data.ModifyRow createDataModifyRow() {
        return new Data.ModifyRow();
    }

    /**
     * Create an instance of {@link Data.DeleteRow }
     * 
     */
    public Data.DeleteRow createDataDeleteRow() {
        return new Data.DeleteRow();
    }

    /**
     * Create an instance of {@link Properties.Map }
     * 
     */
    public Properties.Map createPropertiesMap() {
        return new Properties.Map();
    }

    /**
     * Create an instance of {@link Properties.SyncProvider }
     * 
     */
    public Properties.SyncProvider createPropertiesSyncProvider() {
        return new Properties.SyncProvider();
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link Object }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://java.sun.com/xml/ns/jdbc", name = "updateValue")
    public JAXBElement<Object> createUpdateValue(Object value) {
        return new JAXBElement<Object>(_UpdateValue_QNAME, Object.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link Object }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://java.sun.com/xml/ns/jdbc", name = "columnValue")
    public JAXBElement<Object> createColumnValue(Object value) {
        return new JAXBElement<Object>(_ColumnValue_QNAME, Object.class, null, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://java.sun.com/xml/ns/jdbc", name = "type", scope = Properties.Map.class)
    public JAXBElement<String> createPropertiesMapType(String value) {
        return new JAXBElement<String>(_PropertiesMapType_QNAME, String.class, Properties.Map.class, value);
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link String }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "http://java.sun.com/xml/ns/jdbc", name = "class", scope = Properties.Map.class)
    public JAXBElement<String> createPropertiesMapClass(String value) {
        return new JAXBElement<String>(_PropertiesMapClass_QNAME, String.class, Properties.Map.class, value);
    }

}
