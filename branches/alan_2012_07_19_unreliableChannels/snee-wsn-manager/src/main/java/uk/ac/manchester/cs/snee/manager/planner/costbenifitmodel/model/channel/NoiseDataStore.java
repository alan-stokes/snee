package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.channel;

import java.text.DecimalFormat;

public class NoiseDataStore
{
  private double RSSI;
  private int noiseValue;
  private double signalNoiseRatio;
  private double prr;
  private boolean recieved;
  private String sourceID;
  
  public NoiseDataStore(double RSSI, int noiseValue, double signalNoiseRatio,
                        Double prr, boolean recieved, String sourceID)
  {
    this.recieved = recieved;
    this.RSSI = RSSI;
    this.noiseValue = noiseValue;
    this.signalNoiseRatio = signalNoiseRatio;
    this.prr = prr;
    this.sourceID = sourceID;
  }
  
  public String toString()
  {
    DecimalFormat df = new DecimalFormat("#.#####");
    String result;
    if(recieved)
      result = "T";
    else
      result = "F";
    
    if(prr > 1)
      prr = 1;
    if(prr < -1)
      prr = -1;
    
    return "SID=" + sourceID + " RSSI=" + df.format(RSSI) + " Noise=" + noiseValue + " SNR=" +  
    df.format(signalNoiseRatio) + " prr=" +  df.format(prr) + " r=" + result;
    
    
  }
  
}
