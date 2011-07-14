
package eu.semsorgrid4env.service.stream.push;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import eu.semsorgrid4env.service.stream.StreamPropertyDocumentType;
import org.oasis_open.docs.wsn.b_2.NotificationProducerRP;


/**
 * <p>Java class for PushStreamPropertyDocumentType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="PushStreamPropertyDocumentType">
 *   &lt;complexContent>
 *     &lt;extension base="{http://www.semsorgrid4env.eu/namespace/2009/10/SDS}StreamPropertyDocumentType">
 *       &lt;sequence>
 *         &lt;element ref="{http://docs.oasis-open.org/wsn/b-2}NotificationProducerRP"/>
 *         &lt;element ref="{http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Push}PublicationPolicy" minOccurs="0"/>
 *         &lt;element ref="{http://www.semsorgrid4env.eu/namespace/2009/10/SDS/Push}SubscriptionPolicy" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/extension>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "PushStreamPropertyDocumentType", propOrder = {
    "notificationProducerRP",
    "publicationPolicy",
    "subscriptionPolicy"
})
public class PushStreamPropertyDocumentType
    extends StreamPropertyDocumentType
{

    @XmlElement(name = "NotificationProducerRP", namespace = "http://docs.oasis-open.org/wsn/b-2", required = true)
    protected NotificationProducerRP notificationProducerRP;
    @XmlElement(name = "PublicationPolicy")
    protected PublicationPolicyType publicationPolicy;
    @XmlElement(name = "SubscriptionPolicy")
    protected SubscriptionPolicyType subscriptionPolicy;

    /**
     * Gets the value of the notificationProducerRP property.
     * 
     * @return
     *     possible object is
     *     {@link NotificationProducerRP }
     *     
     */
    public NotificationProducerRP getNotificationProducerRP() {
        return notificationProducerRP;
    }

    /**
     * Sets the value of the notificationProducerRP property.
     * 
     * @param value
     *     allowed object is
     *     {@link NotificationProducerRP }
     *     
     */
    public void setNotificationProducerRP(NotificationProducerRP value) {
        this.notificationProducerRP = value;
    }

    /**
     * Gets the value of the publicationPolicy property.
     * 
     * @return
     *     possible object is
     *     {@link PublicationPolicyType }
     *     
     */
    public PublicationPolicyType getPublicationPolicy() {
        return publicationPolicy;
    }

    /**
     * Sets the value of the publicationPolicy property.
     * 
     * @param value
     *     allowed object is
     *     {@link PublicationPolicyType }
     *     
     */
    public void setPublicationPolicy(PublicationPolicyType value) {
        this.publicationPolicy = value;
    }

    /**
     * Gets the value of the subscriptionPolicy property.
     * 
     * @return
     *     possible object is
     *     {@link SubscriptionPolicyType }
     *     
     */
    public SubscriptionPolicyType getSubscriptionPolicy() {
        return subscriptionPolicy;
    }

    /**
     * Sets the value of the subscriptionPolicy property.
     * 
     * @param value
     *     allowed object is
     *     {@link SubscriptionPolicyType }
     *     
     */
    public void setSubscriptionPolicy(SubscriptionPolicyType value) {
        this.subscriptionPolicy = value;
    }

}
