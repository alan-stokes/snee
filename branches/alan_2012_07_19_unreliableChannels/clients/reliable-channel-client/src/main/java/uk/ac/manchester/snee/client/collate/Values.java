package uk.ac.manchester.snee.client.collate;
public class Values
{
  private Double staticPercentage = null;
  private Double adaptivePercentage = null;
  private Double aggreStaticPercentage = null;
  private Double aggreAdaptivePercentage = null;
 
  public Values()
  {
   
  }

public void setStaticPercentage(Double staticPercentage) {
	this.staticPercentage = staticPercentage;
}

public Double getStaticPercentage() {
	return staticPercentage;
}

public void setAdaptivePercentage(Double adaptivePercentage) {
	this.adaptivePercentage = adaptivePercentage;
}

public Double getAdaptivePercentage() {
	return adaptivePercentage;
}

public void setAggreStaticPercentage(Double aggreStaticPercentage) {
	this.aggreStaticPercentage = aggreStaticPercentage;
}

public Double getAggreStaticPercentage() {
	return aggreStaticPercentage;
}

public void setAggreAdaptivePercentage(Double aggreAdaptivePercentage) {
	this.aggreAdaptivePercentage = aggreAdaptivePercentage;
}

public Double getAggreAdaptivePercentage() {
	return aggreAdaptivePercentage;
}
 
}
