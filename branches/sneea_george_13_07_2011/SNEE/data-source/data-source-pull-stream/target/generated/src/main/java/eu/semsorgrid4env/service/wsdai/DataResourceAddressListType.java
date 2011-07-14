
package eu.semsorgrid4env.service.wsdai;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for DataResourceAddressListType complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="DataResourceAddressListType">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element ref="{http://www.ggf.org/namespaces/2005/12/WS-DAI}DataResourceAddress" maxOccurs="unbounded"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "DataResourceAddressListType", propOrder = {
    "dataResourceAddress"
})
public class DataResourceAddressListType {

    @XmlElement(name = "DataResourceAddress", required = true)
    protected List<DataResourceAddressType> dataResourceAddress;

    /**
     * Gets the value of the dataResourceAddress property.
     * 
     * <p>
     * This accessor method returns a reference to the live list,
     * not a snapshot. Therefore any modification you make to the
     * returned list will be present inside the JAXB object.
     * This is why there is not a <CODE>set</CODE> method for the dataResourceAddress property.
     * 
     * <p>
     * For example, to add a new item, do as follows:
     * <pre>
     *    getDataResourceAddress().add(newItem);
     * </pre>
     * 
     * 
     * <p>
     * Objects of the following type(s) are allowed in the list
     * {@link DataResourceAddressType }
     * 
     * 
     */
    public List<DataResourceAddressType> getDataResourceAddress() {
        if (dataResourceAddress == null) {
            dataResourceAddress = new ArrayList<DataResourceAddressType>();
        }
        return this.dataResourceAddress;
    }

}
