
package com.sun.java.xml.ns.jdbc;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
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
 *         &lt;element name="column-count" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;choice>
 *           &lt;element name="column-definition" maxOccurs="unbounded" minOccurs="0">
 *             &lt;complexType>
 *               &lt;complexContent>
 *                 &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                   &lt;sequence>
 *                     &lt;element name="column-index" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                     &lt;element name="auto-increment" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                     &lt;element name="case-sensitive" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                     &lt;element name="currency" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                     &lt;element name="nullable" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                     &lt;element name="signed" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                     &lt;element name="searchable" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                     &lt;element name="column-display-size" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                     &lt;element name="column-label" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                     &lt;element name="column-name" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                     &lt;element name="schema-name" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                     &lt;element name="column-precision" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                     &lt;element name="column-scale" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                     &lt;element name="table-name" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                     &lt;element name="catalog-name" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                     &lt;element name="column-type" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                     &lt;element name="column-type-name" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *                   &lt;/sequence>
 *                 &lt;/restriction>
 *               &lt;/complexContent>
 *             &lt;/complexType>
 *           &lt;/element>
 *         &lt;/choice>
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
    "columnCount",
    "columnDefinition"
})
@XmlRootElement(name = "metadata")
public class Metadata {

    @XmlElement(name = "column-count", required = true)
    protected String columnCount;
    @XmlElement(name = "column-definition")
    protected List<Metadata.ColumnDefinition> columnDefinition;

    /**
     * Gets the value of the columnCount property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getColumnCount() {
        return columnCount;
    }

    /**
     * Sets the value of the columnCount property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setColumnCount(String value) {
        this.columnCount = value;
    }

    /**
     * Gets the value of the columnDefinition property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the columnDefinition property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getColumnDefinition().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link Metadata.ColumnDefinition }
     * 
     * 
     */
    public List<Metadata.ColumnDefinition> getColumnDefinition() {
        if (columnDefinition == null) {
            columnDefinition = new ArrayList<Metadata.ColumnDefinition>();
        }
        return this.columnDefinition;
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
     *         &lt;element name="column-index" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *         &lt;element name="auto-increment" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *         &lt;element name="case-sensitive" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *         &lt;element name="currency" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *         &lt;element name="nullable" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *         &lt;element name="signed" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *         &lt;element name="searchable" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *         &lt;element name="column-display-size" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *         &lt;element name="column-label" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *         &lt;element name="column-name" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *         &lt;element name="schema-name" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *         &lt;element name="column-precision" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *         &lt;element name="column-scale" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *         &lt;element name="table-name" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *         &lt;element name="catalog-name" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *         &lt;element name="column-type" type="{http://www.w3.org/2001/XMLSchema}string"/>
     *         &lt;element name="column-type-name" type="{http://www.w3.org/2001/XMLSchema}string"/>
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
        "columnIndex",
        "autoIncrement",
        "caseSensitive",
        "currency",
        "nullable",
        "signed",
        "searchable",
        "columnDisplaySize",
        "columnLabel",
        "columnName",
        "schemaName",
        "columnPrecision",
        "columnScale",
        "tableName",
        "catalogName",
        "columnType",
        "columnTypeName"
    })
    public static class ColumnDefinition {

        @XmlElement(name = "column-index", required = true)
        protected String columnIndex;
        @XmlElement(name = "auto-increment", required = true)
        protected String autoIncrement;
        @XmlElement(name = "case-sensitive", required = true)
        protected String caseSensitive;
        @XmlElement(required = true)
        protected String currency;
        @XmlElement(required = true)
        protected String nullable;
        @XmlElement(required = true)
        protected String signed;
        @XmlElement(required = true)
        protected String searchable;
        @XmlElement(name = "column-display-size", required = true)
        protected String columnDisplaySize;
        @XmlElement(name = "column-label", required = true)
        protected String columnLabel;
        @XmlElement(name = "column-name", required = true)
        protected String columnName;
        @XmlElement(name = "schema-name", required = true)
        protected String schemaName;
        @XmlElement(name = "column-precision", required = true)
        protected String columnPrecision;
        @XmlElement(name = "column-scale", required = true)
        protected String columnScale;
        @XmlElement(name = "table-name", required = true)
        protected String tableName;
        @XmlElement(name = "catalog-name", required = true)
        protected String catalogName;
        @XmlElement(name = "column-type", required = true)
        protected String columnType;
        @XmlElement(name = "column-type-name", required = true)
        protected String columnTypeName;

        /**
         * Gets the value of the columnIndex property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getColumnIndex() {
            return columnIndex;
        }

        /**
         * Sets the value of the columnIndex property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setColumnIndex(String value) {
            this.columnIndex = value;
        }

        /**
         * Gets the value of the autoIncrement property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getAutoIncrement() {
            return autoIncrement;
        }

        /**
         * Sets the value of the autoIncrement property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setAutoIncrement(String value) {
            this.autoIncrement = value;
        }

        /**
         * Gets the value of the caseSensitive property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getCaseSensitive() {
            return caseSensitive;
        }

        /**
         * Sets the value of the caseSensitive property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setCaseSensitive(String value) {
            this.caseSensitive = value;
        }

        /**
         * Gets the value of the currency property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getCurrency() {
            return currency;
        }

        /**
         * Sets the value of the currency property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setCurrency(String value) {
            this.currency = value;
        }

        /**
         * Gets the value of the nullable property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getNullable() {
            return nullable;
        }

        /**
         * Sets the value of the nullable property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setNullable(String value) {
            this.nullable = value;
        }

        /**
         * Gets the value of the signed property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getSigned() {
            return signed;
        }

        /**
         * Sets the value of the signed property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setSigned(String value) {
            this.signed = value;
        }

        /**
         * Gets the value of the searchable property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getSearchable() {
            return searchable;
        }

        /**
         * Sets the value of the searchable property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setSearchable(String value) {
            this.searchable = value;
        }

        /**
         * Gets the value of the columnDisplaySize property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getColumnDisplaySize() {
            return columnDisplaySize;
        }

        /**
         * Sets the value of the columnDisplaySize property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setColumnDisplaySize(String value) {
            this.columnDisplaySize = value;
        }

        /**
         * Gets the value of the columnLabel property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getColumnLabel() {
            return columnLabel;
        }

        /**
         * Sets the value of the columnLabel property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setColumnLabel(String value) {
            this.columnLabel = value;
        }

        /**
         * Gets the value of the columnName property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getColumnName() {
            return columnName;
        }

        /**
         * Sets the value of the columnName property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setColumnName(String value) {
            this.columnName = value;
        }

        /**
         * Gets the value of the schemaName property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getSchemaName() {
            return schemaName;
        }

        /**
         * Sets the value of the schemaName property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setSchemaName(String value) {
            this.schemaName = value;
        }

        /**
         * Gets the value of the columnPrecision property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getColumnPrecision() {
            return columnPrecision;
        }

        /**
         * Sets the value of the columnPrecision property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setColumnPrecision(String value) {
            this.columnPrecision = value;
        }

        /**
         * Gets the value of the columnScale property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getColumnScale() {
            return columnScale;
        }

        /**
         * Sets the value of the columnScale property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setColumnScale(String value) {
            this.columnScale = value;
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
         * Gets the value of the catalogName property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getCatalogName() {
            return catalogName;
        }

        /**
         * Sets the value of the catalogName property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setCatalogName(String value) {
            this.catalogName = value;
        }

        /**
         * Gets the value of the columnType property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getColumnType() {
            return columnType;
        }

        /**
         * Sets the value of the columnType property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setColumnType(String value) {
            this.columnType = value;
        }

        /**
         * Gets the value of the columnTypeName property.
         * 
         * @return
         *     possible object is
         *     {@link String }
         *     
         */
        public String getColumnTypeName() {
            return columnTypeName;
        }

        /**
         * Sets the value of the columnTypeName property.
         * 
         * @param value
         *     allowed object is
         *     {@link String }
         *     
         */
        public void setColumnTypeName(String value) {
            this.columnTypeName = value;
        }

    }

}
