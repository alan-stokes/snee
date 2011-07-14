
package com.sun.java.xml.ns.jdbc;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlElementRefs;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for anonymous complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType>
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="command" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="concurrency" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="datasource" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="escape-processing" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="fetch-direction" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="fetch-size" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="isolation-level" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="key-columns">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence maxOccurs="unbounded" minOccurs="0">
 *                   &lt;element name="column" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                 &lt;/sequence>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="map">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence maxOccurs="unbounded" minOccurs="0">
 *                   &lt;element name="type" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                   &lt;element name="class" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                 &lt;/sequence>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="max-field-size" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="max-rows" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="query-timeout" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="read-only" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="rowset-type" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="show-deleted" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="table-name" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="url" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="sync-provider">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;element name="sync-provider-name" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                   &lt;element name="sync-provider-vendor" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                   &lt;element name="sync-provider-version" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                   &lt;element name="sync-provider-grade" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                   &lt;element name="data-source-lock" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                 &lt;/sequence>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "command",
    "concurrency",
    "datasource",
    "escapeProcessing",
    "fetchDirection",
    "fetchSize",
    "isolationLevel",
    "keyColumns",
    "map",
    "maxFieldSize",
    "maxRows",
    "queryTimeout",
    "readOnly",
    "rowsetType",
    "showDeleted",
    "tableName",
    "url",
    "syncProvider"
})
@XmlRootElement(name = "properties")
public class Properties {

    @XmlElement(required = true)
    protected String command;
    @XmlElement(required = true)
    protected String concurrency;
    @XmlElement(required = true)
    protected String datasource;
    @XmlElement(name = "escape-processing", required = true)
    protected String escapeProcessing;
    @XmlElement(name = "fetch-direction", required = true)
    protected String fetchDirection;
    @XmlElement(name = "fetch-size", required = true)
    protected String fetchSize;
    @XmlElement(name = "isolation-level", required = true)
    protected String isolationLevel;
    @XmlElement(name = "key-columns", required = true)
    protected Properties.KeyColumns keyColumns;
    @XmlElement(required = true)
    protected Properties.Map map;
    @XmlElement(name = "max-field-size", required = true)
    protected String maxFieldSize;
    @XmlElement(name = "max-rows", required = true)
    protected String maxRows;
    @XmlElement(name = "query-timeout", required = true)
    protected String queryTimeout;
    @XmlElement(name = "read-only", required = true)
    protected String readOnly;
    @XmlElement(name = "rowset-type", required = true)
    protected String rowsetType;
    @XmlElement(name = "show-deleted", required = true)
    protected String showDeleted;
    @XmlElement(name = "table-name", required = true)
    protected String tableName;
    @XmlElement(required = true)
    protected String url;
    @XmlElement(name = "sync-provider", required = true)
    protected Properties.SyncProvider syncProvider;

    /**
     * Gets the value of the command property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getCommand() {
        return command;
    }

    /**
     * Sets the value of the command property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setCommand(String value) {
        this.command = value;
    }

    /**
     * Gets the value of the concurrency property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getConcurrency() {
        return concurrency;
    }

    /**
     * Sets the value of the concurrency property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setConcurrency(String value) {
        this.concurrency = value;
    }

    /**
     * Gets the value of the datasource property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getDatasource() {
        return datasource;
    }

    /**
     * Sets the value of the datasource property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setDatasource(String value) {
        this.datasource = value;
    }

    /**
     * Gets the value of the escapeProcessing property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getEscapeProcessing() {
        return escapeProcessing;
    }

    /**
     * Sets the value of the escapeProcessing property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setEscapeProcessing(String value) {
        this.escapeProcessing = value;
    }

    /**
     * Gets the value of the fetchDirection property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getFetchDirection() {
        return fetchDirection;
    }

    /**
     * Sets the value of the fetchDirection property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setFetchDirection(String value) {
        this.fetchDirection = value;
    }

    /**
     * Gets the value of the fetchSize property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getFetchSize() {
        return fetchSize;
    }

    /**
     * Sets the value of the fetchSize property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setFetchSize(String value) {
        this.fetchSize = value;
    }

    /**
     * Gets the value of the isolationLevel property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getIsolationLevel() {
        return isolationLevel;
    }

    /**
     * Sets the value of the isolationLevel property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setIsolationLevel(String value) {
        this.isolationLevel = value;
    }

    /**
     * Gets the value of the keyColumns property.
     * 
     * @return
     *     possible object is
     *     {@link Properties.KeyColumns }
     *     
     */
    public Properties.KeyColumns getKeyColumns() {
        return keyColumns;
    }

    /**
     * Sets the value of the keyColumns property.
     * 
     * @param value
     *     allowed object is
     *     {@link Properties.KeyColumns }
     *     
     */
    public void setKeyColumns(Properties.KeyColumns value) {
        this.keyColumns = value;
    }

    /**
     * Gets the value of the map property.
     * 
     * @return
     *     possible object is
     *     {@link Properties.Map }
     *     
     */
    public Properties.Map getMap() {
        return map;
    }

    /**
     * Sets the value of the map property.
     * 
     * @param value
     *     allowed object is
     *     {@link Properties.Map }
     *     
     */
    public void setMap(Properties.Map value) {
        this.map = value;
    }

    /**
     * Gets the value of the maxFieldSize property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getMaxFieldSize() {
        return maxFieldSize;
    }

    /**
     * Sets the value of the maxFieldSize property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setMaxFieldSize(String value) {
        this.maxFieldSize = value;
    }

    /**
     * Gets the value of the maxRows property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getMaxRows() {
        return maxRows;
    }

    /**
     * Sets the value of the maxRows property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setMaxRows(String value) {
        this.maxRows = value;
    }

    /**
     * Gets the value of the queryTimeout property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getQueryTimeout() {
        return queryTimeout;
    }

    /**
     * Sets the value of the queryTimeout property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setQueryTimeout(String value) {
        this.queryTimeout = value;
    }

    /**
     * Gets the value of the readOnly property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getReadOnly() {
        return readOnly;
    }

    /**
     * Sets the value of the readOnly property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setReadOnly(String value) {
        this.readOnly = value;
    }

    /**
     * Gets the value of the rowsetType property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getRowsetType() {
        return rowsetType;
    }

    /**
     * Sets the value of the rowsetType property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setRowsetType(String value) {
        this.rowsetType = value;
    }

    /**
     * Gets the value of the showDeleted property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getShowDeleted() {
        return showDeleted;
    }

    /**
     * Sets the value of the showDeleted property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setShowDeleted(String value) {
        this.showDeleted = value;
    }

    /**
     * Gets the value of the tableName property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Sets the value of the tableName property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setTableName(String value) {
        this.tableName = value;
    }

    /**
     * Gets the value of the url property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getUrl() {
        return url;
    }

    /**
     * Sets the value of the url property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setUrl(String value) {
        this.url = value;
    }

    /**
     * Gets the value of the syncProvider property.
     * 
     * @return
     *     possible object is
     *     {@link Properties.SyncProvider }
     *     
     */
    public Properties.SyncProvider getSyncProvider() {
        return syncProvider;
    }

    /**
     * Sets the value of the syncProvider property.
     * 
     * @param value
     *     allowed object is
     *     {@link Properties.SyncProvider }
     *     
     */
    public void setSyncProvider(Properties.SyncProvider value) {
        this.syncProvider = value;
    }


    /**
     * <p>Java class for anonymous complex type.
     * 
     * <p>The following schema fragment specifies the expected content contained within this class.
     * 
     * <pre>
     * &lt;complexType>
     *   &lt;complexContent>
     *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
     *       &lt;sequence maxOccurs="unbounded" minOccurs="0">
     *         &lt;element name="column" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *       &lt;/sequence>
     *     &lt;/restriction>
     *   &lt;/complexContent>
     * &lt;/complexType>
     * </pre>
     * 
     * 
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "", propOrder = {
        "column"
    })
    public static class KeyColumns {

        protected List<String> column;

        /**
         * Gets the value of the column property.
         * 
         * <p>
         * This accessor method returns a reference to the live list,
         * not a snapshot. Therefore any modification you make to the
         * returned list will be present inside the JAXB object.
         * This is why there is not a <CODE>set</CODE> method for the column property.
         * 
         * <p>
         * For example, to add a new item, do as follows:
         * <pre>
         *    getColumn().add(newItem);
         * </pre>
         * 
         * 
         * <p>
         * Objects of the following type(s) are allowed in the list
         * {@link String }
         * 
         * 
         */
        public List<String> getColumn() {
            if (column == null) {
                column = new ArrayList<String>();
            }
            return this.column;
        }

    }


    /**
     * <p>Java class for anonymous complex type.
     * 
     * <p>The following schema fragment specifies the expected content contained within this class.
     * 
     * <pre>
     * &lt;complexType>
     *   &lt;complexContent>
     *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
     *       &lt;sequence maxOccurs="unbounded" minOccurs="0">
     *         &lt;element name="type" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *         &lt;element name="class" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *       &lt;/sequence>
     *     &lt;/restriction>
     *   &lt;/complexContent>
     * &lt;/complexType>
     * </pre>
     * 
     * 
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "", propOrder = {
        "typeAndClazz"
    })
    public static class Map {

        @XmlElementRefs({
            @XmlElementRef(name = "type", namespace = "http://java.sun.com/xml/ns/jdbc", type = JAXBElement.class),
            @XmlElementRef(name = "class", namespace = "http://java.sun.com/xml/ns/jdbc", type = JAXBElement.class)
        })
        protected List<JAXBElement<String>> typeAndClazz;

        /**
         * Gets the value of the typeAndClazz property.
         * 
         * <p>
         * This accessor method returns a reference to the live list,
         * not a snapshot. Therefore any modification you make to the
         * returned list will be present inside the JAXB object.
         * This is why there is not a <CODE>set</CODE> method for the typeAndClazz property.
         * 
         * <p>
         * For example, to add a new item, do as follows:
         * <pre>
         *    getTypeAndClazz().add(newItem);
         * </pre>
         * 
         * 
         * <p>
         * Objects of the following type(s) are allowed in the list
         * {@link JAXBElement }{@code <}{@link String }{@code >}
         * {@link JAXBElement }{@code <}{@link String }{@code >}
         * 
         * 
         */
        public List<JAXBElement<String>> getTypeAndClazz() {
            if (typeAndClazz == null) {
                typeAndClazz = new ArrayList<JAXBElement<String>>();
            }
            return this.typeAndClazz;
        }

    }


    /**
     * <p>Java class for anonymous complex type.
     * 
     * <p>The following schema fragment specifies the expected content contained within this class.
     * 
     * <pre>
     * &lt;complexType>
     *   &lt;complexContent>
     *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
     *       &lt;sequence>
     *         &lt;element name="sync-provider-name" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *         &lt;element name="sync-provider-vendor" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *         &lt;element name="sync-provider-version" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *         &lt;element name="sync-provider-grade" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *         &lt;element name="data-source-lock" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *       &lt;/sequence>
     *     &lt;/restriction>
     *   &lt;/complexContent>
     * &lt;/complexType>
     * </pre>
     * 
     * 
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "", propOrder = {
        "syncProviderName",
        "syncProviderVendor",
        "syncProviderVersion",
        "syncProviderGrade",
        "dataSourceLock"
    })
    public static class SyncProvider {

        @XmlElement(name = "sync-provider-name", required = true)
        protected String syncProviderName;
        @XmlElement(name = "sync-provider-vendor", required = true)
        protected String syncProviderVendor;
        @XmlElement(name = "sync-provider-version", required = true)
        protected String syncProviderVersion;
        @XmlElement(name = "sync-provider-grade", required = true)
        protected String syncProviderGrade;
        @XmlElement(name = "data-source-lock", required = true)
        protected String dataSourceLock;

        /**
         * Gets the value of the syncProviderName property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getSyncProviderName() {
            return syncProviderName;
        }

        /**
         * Sets the value of the syncProviderName property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setSyncProviderName(String value) {
            this.syncProviderName = value;
        }

        /**
         * Gets the value of the syncProviderVendor property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getSyncProviderVendor() {
            return syncProviderVendor;
        }

        /**
         * Sets the value of the syncProviderVendor property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setSyncProviderVendor(String value) {
            this.syncProviderVendor = value;
        }

        /**
         * Gets the value of the syncProviderVersion property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getSyncProviderVersion() {
            return syncProviderVersion;
        }

        /**
         * Sets the value of the syncProviderVersion property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setSyncProviderVersion(String value) {
            this.syncProviderVersion = value;
        }

        /**
         * Gets the value of the syncProviderGrade property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getSyncProviderGrade() {
            return syncProviderGrade;
        }

        /**
         * Sets the value of the syncProviderGrade property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setSyncProviderGrade(String value) {
            this.syncProviderGrade = value;
        }

        /**
         * Gets the value of the dataSourceLock property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getDataSourceLock() {
            return dataSourceLock;
        }

        /**
         * Sets the value of the dataSourceLock property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setDataSourceLock(String value) {
            this.dataSourceLock = value;
        }

    }

}
