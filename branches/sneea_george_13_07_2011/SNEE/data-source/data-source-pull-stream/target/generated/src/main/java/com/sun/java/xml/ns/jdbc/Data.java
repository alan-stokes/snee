
package com.sun.java.xml.ns.jdbc;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlElementRefs;
import javax.xml.bind.annotation.XmlElements;
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
 *       &lt;sequence maxOccurs="unbounded" minOccurs="0">
 *         &lt;element name="currentRow" maxOccurs="unbounded" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence maxOccurs="unbounded" minOccurs="0">
 *                   &lt;element ref="{http://java.sun.com/xml/ns/jdbc}columnValue"/>
 *                 &lt;/sequence>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="insertRow" maxOccurs="unbounded" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;choice maxOccurs="unbounded" minOccurs="0">
 *                   &lt;element ref="{http://java.sun.com/xml/ns/jdbc}columnValue"/>
 *                   &lt;element ref="{http://java.sun.com/xml/ns/jdbc}updateValue"/>
 *                 &lt;/choice>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="deleteRow" maxOccurs="unbounded" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence maxOccurs="unbounded" minOccurs="0">
 *                   &lt;element ref="{http://java.sun.com/xml/ns/jdbc}columnValue"/>
 *                   &lt;element ref="{http://java.sun.com/xml/ns/jdbc}updateValue"/>
 *                 &lt;/sequence>
 *               &lt;/restriction>
 *             &lt;/complexContent>
 *           &lt;/complexType>
 *         &lt;/element>
 *         &lt;element name="modifyRow" maxOccurs="unbounded" minOccurs="0">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence maxOccurs="unbounded" minOccurs="0">
 *                   &lt;element ref="{http://java.sun.com/xml/ns/jdbc}columnValue"/>
 *                   &lt;element ref="{http://java.sun.com/xml/ns/jdbc}updateValue"/>
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
    "currentRowAndInsertRowAndDeleteRow"
})
@XmlRootElement(name = "data")
public class Data {

    @XmlElements({
        @XmlElement(name = "deleteRow", type = Data.DeleteRow.class),
        @XmlElement(name = "currentRow", type = Data.CurrentRow.class),
        @XmlElement(name = "modifyRow", type = Data.ModifyRow.class),
        @XmlElement(name = "insertRow", type = Data.InsertRow.class)
    })
    protected List<Object> currentRowAndInsertRowAndDeleteRow;

    /**
     * Gets the value of the currentRowAndInsertRowAndDeleteRow property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the currentRowAndInsertRowAndDeleteRow property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getCurrentRowAndInsertRowAndDeleteRow().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link Data.DeleteRow }
     * {@link Data.CurrentRow }
     * {@link Data.ModifyRow }
     * {@link Data.InsertRow }
     * 
     * 
     */
    public List<Object> getCurrentRowAndInsertRowAndDeleteRow() {
        if (currentRowAndInsertRowAndDeleteRow == null) {
            currentRowAndInsertRowAndDeleteRow = new ArrayList<Object>();
        }
        return this.currentRowAndInsertRowAndDeleteRow;
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
     *         &lt;element ref="{http://java.sun.com/xml/ns/jdbc}columnValue"/>
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
        "columnValue"
    })
    public static class CurrentRow {

        protected List<Object> columnValue;

        /**
         * Gets the value of the columnValue property.
         * 
         * <p>
         * This accessor method returns a reference to the live list,
         * not a snapshot. Therefore any modification you make to the
         * returned list will be present inside the JAXB object.
         * This is why there is not a <CODE>set</CODE> method for the columnValue property.
         * 
         * <p>
         * For example, to add a new item, do as follows:
         * <pre>
         *    getColumnValue().add(newItem);
         * </pre>
         * 
         * 
         * <p>
         * Objects of the following type(s) are allowed in the list
         * {@link Object }
         * 
         * 
         */
        public List<Object> getColumnValue() {
            if (columnValue == null) {
                columnValue = new ArrayList<Object>();
            }
            return this.columnValue;
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
     *         &lt;element ref="{http://java.sun.com/xml/ns/jdbc}columnValue"/>
     *         &lt;element ref="{http://java.sun.com/xml/ns/jdbc}updateValue"/>
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
        "columnValueAndUpdateValue"
    })
    public static class DeleteRow {

        @XmlElementRefs({
            @XmlElementRef(name = "updateValue", namespace = "http://java.sun.com/xml/ns/jdbc", type = JAXBElement.class),
            @XmlElementRef(name = "columnValue", namespace = "http://java.sun.com/xml/ns/jdbc", type = JAXBElement.class)
        })
        protected List<JAXBElement<Object>> columnValueAndUpdateValue;

        /**
         * Gets the value of the columnValueAndUpdateValue property.
         * 
         * <p>
         * This accessor method returns a reference to the live list,
         * not a snapshot. Therefore any modification you make to the
         * returned list will be present inside the JAXB object.
         * This is why there is not a <CODE>set</CODE> method for the columnValueAndUpdateValue property.
         * 
         * <p>
         * For example, to add a new item, do as follows:
         * <pre>
         *    getColumnValueAndUpdateValue().add(newItem);
         * </pre>
         * 
         * 
         * <p>
         * Objects of the following type(s) are allowed in the list
         * {@link JAXBElement }{@code <}{@link Object }{@code >}
         * {@link JAXBElement }{@code <}{@link Object }{@code >}
         * 
         * 
         */
        public List<JAXBElement<Object>> getColumnValueAndUpdateValue() {
            if (columnValueAndUpdateValue == null) {
                columnValueAndUpdateValue = new ArrayList<JAXBElement<Object>>();
            }
            return this.columnValueAndUpdateValue;
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
     *       &lt;choice maxOccurs="unbounded" minOccurs="0">
     *         &lt;element ref="{http://java.sun.com/xml/ns/jdbc}columnValue"/>
     *         &lt;element ref="{http://java.sun.com/xml/ns/jdbc}updateValue"/>
     *       &lt;/choice>
     *     &lt;/restriction>
     *   &lt;/complexContent>
     * &lt;/complexType>
     * </pre>
     * 
     * 
     */
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "", propOrder = {
        "columnValueOrUpdateValue"
    })
    public static class InsertRow {

        @XmlElementRefs({
            @XmlElementRef(name = "updateValue", namespace = "http://java.sun.com/xml/ns/jdbc", type = JAXBElement.class),
            @XmlElementRef(name = "columnValue", namespace = "http://java.sun.com/xml/ns/jdbc", type = JAXBElement.class)
        })
        protected List<JAXBElement<Object>> columnValueOrUpdateValue;

        /**
         * Gets the value of the columnValueOrUpdateValue property.
         * 
         * <p>
         * This accessor method returns a reference to the live list,
         * not a snapshot. Therefore any modification you make to the
         * returned list will be present inside the JAXB object.
         * This is why there is not a <CODE>set</CODE> method for the columnValueOrUpdateValue property.
         * 
         * <p>
         * For example, to add a new item, do as follows:
         * <pre>
         *    getColumnValueOrUpdateValue().add(newItem);
         * </pre>
         * 
         * 
         * <p>
         * Objects of the following type(s) are allowed in the list
         * {@link JAXBElement }{@code <}{@link Object }{@code >}
         * {@link JAXBElement }{@code <}{@link Object }{@code >}
         * 
         * 
         */
        public List<JAXBElement<Object>> getColumnValueOrUpdateValue() {
            if (columnValueOrUpdateValue == null) {
                columnValueOrUpdateValue = new ArrayList<JAXBElement<Object>>();
            }
            return this.columnValueOrUpdateValue;
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
     *         &lt;element ref="{http://java.sun.com/xml/ns/jdbc}columnValue"/>
     *         &lt;element ref="{http://java.sun.com/xml/ns/jdbc}updateValue"/>
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
        "columnValueAndUpdateValue"
    })
    public static class ModifyRow {

        @XmlElementRefs({
            @XmlElementRef(name = "updateValue", namespace = "http://java.sun.com/xml/ns/jdbc", type = JAXBElement.class),
            @XmlElementRef(name = "columnValue", namespace = "http://java.sun.com/xml/ns/jdbc", type = JAXBElement.class)
        })
        protected List<JAXBElement<Object>> columnValueAndUpdateValue;

        /**
         * Gets the value of the columnValueAndUpdateValue property.
         * 
         * <p>
         * This accessor method returns a reference to the live list,
         * not a snapshot. Therefore any modification you make to the
         * returned list will be present inside the JAXB object.
         * This is why there is not a <CODE>set</CODE> method for the columnValueAndUpdateValue property.
         * 
         * <p>
         * For example, to add a new item, do as follows:
         * <pre>
         *    getColumnValueAndUpdateValue().add(newItem);
         * </pre>
         * 
         * 
         * <p>
         * Objects of the following type(s) are allowed in the list
         * {@link JAXBElement }{@code <}{@link Object }{@code >}
         * {@link JAXBElement }{@code <}{@link Object }{@code >}
         * 
         * 
         */
        public List<JAXBElement<Object>> getColumnValueAndUpdateValue() {
            if (columnValueAndUpdateValue == null) {
                columnValueAndUpdateValue = new ArrayList<JAXBElement<Object>>();
            }
            return this.columnValueAndUpdateValue;
        }

    }

}
