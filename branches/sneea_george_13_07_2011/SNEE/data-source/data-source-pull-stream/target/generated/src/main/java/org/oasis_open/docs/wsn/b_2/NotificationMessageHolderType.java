
package org.oasis_open.docs.wsn.b_2;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAnyElement;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import org.apache.cxf.ws.addressing.EndpointReferenceType;
import org.w3c.dom.Element;


/**
 * <p>Java class for NotificationMessageHolderType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="NotificationMessageHolderType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element ref="{http://docs.oasis-open.org/wsn/b-2}SubscriptionReference" minOccurs="0"/>
 *         &lt;element ref="{http://docs.oasis-open.org/wsn/b-2}Topic" minOccurs="0"/>
 *         &lt;element ref="{http://docs.oasis-open.org/wsn/b-2}ProducerReference" minOccurs="0"/>
 *         &lt;element name="Message">
 *           &lt;complexType>
 *             &lt;complexContent>
 *               &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *                 &lt;sequence>
 *                   &lt;any processContents='lax'/>
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
@XmlType(name = "NotificationMessageHolderType", propOrder = {
    "subscriptionReference",
    "topic",
    "producerReference",
    "message"
})
public class NotificationMessageHolderType {

    @XmlElement(name = "SubscriptionReference")
    protected EndpointReferenceType subscriptionReference;
    @XmlElement(name = "Topic")
    protected TopicExpressionType topic;
    @XmlElement(name = "ProducerReference")
    protected EndpointReferenceType producerReference;
    @XmlElement(name = "Message", required = true)
    protected NotificationMessageHolderType.Message message;

    /**
     * Gets the value of the subscriptionReference property.
     * 
     * @return
     *     possible object is
     *     {@link EndpointReferenceType }
     *     
     */
    public EndpointReferenceType getSubscriptionReference() {
        return subscriptionReference;
    }

    /**
     * Sets the value of the subscriptionReference property.
     * 
     * @param value
     *     allowed object is
     *     {@link EndpointReferenceType }
     *     
     */
    public void setSubscriptionReference(EndpointReferenceType value) {
        this.subscriptionReference = value;
    }

    /**
     * Gets the value of the topic property.
     * 
     * @return
     *     possible object is
     *     {@link TopicExpressionType }
     *     
     */
    public TopicExpressionType getTopic() {
        return topic;
    }

    /**
     * Sets the value of the topic property.
     * 
     * @param value
     *     allowed object is
     *     {@link TopicExpressionType }
     *     
     */
    public void setTopic(TopicExpressionType value) {
        this.topic = value;
    }

    /**
     * Gets the value of the producerReference property.
     * 
     * @return
     *     possible object is
     *     {@link EndpointReferenceType }
     *     
     */
    public EndpointReferenceType getProducerReference() {
        return producerReference;
    }

    /**
     * Sets the value of the producerReference property.
     * 
     * @param value
     *     allowed object is
     *     {@link EndpointReferenceType }
     *     
     */
    public void setProducerReference(EndpointReferenceType value) {
        this.producerReference = value;
    }

    /**
     * Gets the value of the message property.
     * 
     * @return
     *     possible object is
     *     {@link NotificationMessageHolderType.Message }
     *     
     */
    public NotificationMessageHolderType.Message getMessage() {
        return message;
    }

    /**
     * Sets the value of the message property.
     * 
     * @param value
     *     allowed object is
     *     {@link NotificationMessageHolderType.Message }
     *     
     */
    public void setMessage(NotificationMessageHolderType.Message value) {
        this.message = value;
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
     *         &lt;any processContents='lax'/>
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
        "any"
    })
    public static class Message {

        @XmlAnyElement(lax = true)
        protected Object any;

        /**
         * Gets the value of the any property.
         * 
         * @return
         *     possible object is
         *     {@link Element }
         *     {@link Object }
         *     
         */
        public Object getAny() {
            return any;
        }

        /**
         * Sets the value of the any property.
         * 
         * @param value
         *     allowed object is
         *     {@link Element }
         *     {@link Object }
         *     
         */
        public void setAny(Object value) {
            this.any = value;
        }

    }

}
