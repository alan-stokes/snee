package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.channel;

//class ported from Hyungjune lee from stanford. 

import java.io.IOException;
import java.util.Random;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.RadioLink;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;

public class NoiseModel
{
  //default global values.
  private boolean useingClearChannels;
  private boolean relibaleChannels;
  private boolean veriablePower;
  private Double transmissionPower; 
  private CostParameters costs;
  private Double signalFrequencyInMegaHerze;
  private Topology network;
  private CPMModel cpmModel;

  //values used within the RSSI forumla
  private static double pathLossExponent;

  
  
  public NoiseModel(Topology network, CostParameters costs) 
  throws SNEEConfigurationException, IOException
  {
    this.costs = costs;
    this.network = network;
    useingClearChannels = 
      SNEEProperties.getBoolSetting(SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS_CLEANRADIO);
    relibaleChannels = 
       SNEEProperties.getBoolSetting(SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS);
    veriablePower = 
      SNEEProperties.getBoolSetting(SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS_VERIABLEPOWER);
    signalFrequencyInMegaHerze =
      Double.parseDouble(SNEEProperties.getSetting(
          SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS_SIGNALFREQUENCY));
    pathLossExponent = 
      Double.parseDouble(SNEEProperties.getSetting(
          SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS_PATHLOSSEXPONENT));
    if(!veriablePower)
      transmissionPower = 
        Double.parseDouble(SNEEProperties.getSetting(
            SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS_TRANSMISSIONPOWER));
    else
    {
      //TODO impliment some sort of arbitary transmission power. 
    }
    prepareErfc(100,100);
    cpmModel = new CPMModel(network);
  }

  /**
   * entrance to the noise model for channel model.
   * Uses the noise model to determine if a packet has been recieved by 
   * DestID from sourceID though their direct link
   * @param sourceID
   * @param DestID
   * @return
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   */
  public boolean packetRecieved(String sourceID, String destID, Long startTime,
                                ChannelModelSite destSite, Integer packetID)
  throws NumberFormatException, SNEEConfigurationException
  {
    if(useingClearChannels && relibaleChannels)
    {
      return true;
    }
    else
      return didPacketGetRecieved(sourceID, destID, startTime, destSite, packetID);
  }

  /**
   * helper method for the entrance method. 
   * @param sourceID
   * @param destID
   * @return
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   */
  private boolean didPacketGetRecieved(String sourceID, String destID, Long startTime,
                                       ChannelModelSite destSite, Integer packetID)
  throws NumberFormatException, SNEEConfigurationException
  {
    
    /**
     * PRR is caulcated by
     * PRR = (1 - (1/2 * exp - (S.N / 2)(1/0.64))) ^ 8 * packetsize
     * from Analyzing the transitional region in low power wireless links Marco Zuniga and Bhaskar Krishbaneachari
     */
    int packetSize = costs.getMaxMessagePayloadSize() + costs.getPayloadOverhead();
    Integer noise = cpmModel.getNoise(sourceID, startTime);
    Double RSSI = this.getRSSI(sourceID, destID);
    Double signalNoiseRatio = RSSI - noise;
    Double prr = (1-(0.5 * Math.pow(Math.exp(0 - (signalNoiseRatio / 2) * (1/0.64)), 8 * packetSize)));
    //prr from cpm model seems more realistic and less dubious than prr from trnasiotnal paper
 //   Double prr2 = cpmPRRForm(signalNoiseRatio);
    
    Random random = new Random(new Long(0));
    double randomDouble = random.nextDouble();
    if(randomDouble < (prr))
    {
      destSite.addNoiseTrace(sourceID, packetID, 
                             new NoiseDataStore(RSSI, noise, signalNoiseRatio, prr, true,
                                                sourceID));
      return true;
    }
    else
    {
      destSite.addNoiseTrace(sourceID, packetID, 
                             new NoiseDataStore(RSSI, noise, signalNoiseRatio, prr, false,
                                                sourceID));
      return false;
    }
  }

 
/*
  private Double cpmPRRForm(Double signalNoiseRatio)
  {
    double x = signalNoiseRatio - 2.3851;
    double pse = 0.5 * erfc(0.9794 * x / Math.sqrt(2));
    double prr= Math.pow(1-pse, 23*2);
    if(prr > 1)
      prr = 1.1;
    else if(prr <0)
      prr = -0.1;
    return prr;
  }*/

  /**
   * returns the estimated signal strnegth though RSSI given 2 node ids.
   * @param sourceID
   * @param destID
   * @return
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   */
  private Double getRSSI(String sourceID, String destID)
  throws NumberFormatException, SNEEConfigurationException
  {
    RadioLink link = 
      network.getRadioLink(network.getSite(sourceID), network.getSite(destID));
    Double distanceBetweenNodes = link.getDistanceCost();
    double distanceFactor = Double.parseDouble(SNEEProperties.getSetting("distanceFactor"));
    distanceBetweenNodes = distanceBetweenNodes * distanceFactor;
    /**rssi value is determined by RSSI = (PL(D0) + 10 n log (d / d0)
    // d = distance between transmitter and reciever.
     * d0 the reference distance (telosb mote 75m)
     * n path loss exponent (free space = 2, urban area cellular raido 2.7 to 3.5, 
     *                       shaowed urban celluar radio 3 to 5,
     *                       in building line of sight 1.6 to 1.8
     *                       obstructed in building 4 to 6
     *                       obstructed in factories 2 to 3)
    */
    
    /**
     * Free space path loss forumla is used to detemrine PL(d0) 
     * PL(d0) (dB) = 10 log 10 ((((4 * pi ) / light ) * d0 * f) ^2)
     * From wikipedia 
     */
    Double PL_Do_ =  Math.log10(Math.pow((((4 * Math.PI) / 
        NoiseModelConstants.C_LIGHT) * NoiseModelConstants.d0 * signalFrequencyInMegaHerze), 2));
    
    //quassian random variable for path loss forumla.
    Random random = new Random(new Long(0));
    Double quassianRandomVariable = random.nextGaussian();
    quassianRandomVariable = quassianRandomVariable * NoiseModelConstants.RSSI_DISTANCE_DEPENDENT_MEAN;
    /**
     * PL(d) (dB) = PL_d0_ + 10 * n * log(d/d0) + X 
     * From Analyzing the low power wireless links for WSN (md. mainul islam mamun, tarek hasan-al-mahmud,
     *      Typhoon: A reliable data dissemination protocol for wsn (chieh-jan mike liang, Razvan Musaloiu-E
     *      Wireless Communications principles and practice second edition (Theodore S. Rappaport)
     */                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
     
    Double pathLoss = PL_Do_ + (10 * pathLossExponent * 
        Math.log((distanceBetweenNodes / NoiseModelConstants.d0))) + quassianRandomVariable;

    if(!this.veriablePower)
    {
      /** RSSI is measured in dB **/
      Double RSSI = transmissionPower - pathLoss;
      return RSSI;  
    }
    else
    {
      Double energyCost = link.getEnergyCost();
      double perSlot = 24 / 254;
      double additionTOMinimalTransmissionCost = perSlot * energyCost;
      Double nodeTransmissionPower = -24 + additionTOMinimalTransmissionCost;
      Double RSSI = nodeTransmissionPower - pathLoss;
      return RSSI;  
    }
  }

    public static double erfh(double x )
    {
      return erfc(x);
    }
    
    //========================================================================= 
  //Brute force (asymmetric) erfc calculation.
    //------------------------------------------------------------------------- 
  //Enable this function if you don't have Internet and 
    //don't have decent Java Math. package.

    public static double erfc(double x)
    {
      if(x<0)return 1.0-erfc(-x);
        double fraction=x%step;
        int left=Math.max(0,(int)((x-fraction)*step1));
        int right=left+1;
        if(right>=accur)return 0.5;
        //Interpolate:
        return (mErfc[right]-mErfc[left])*(x*step1-left)+mErfc[left];
    }
    
    private static double[] mErfc;
    private static int accur;
    private static double norm;
    private static double step;
    private static double step1;
    //Input: Combination 1000000, 100 of accuracy,subaccuracy
    //       gave an overall integration accuracy 2*10^(-7)
    //       (About 4 seconds of preparation on 2GHz PC).
    //       
    public static void prepareErfc(int accuracy,int subaccuracy)
    {
      accur=accuracy;
    mErfc=new double[accur];
    norm=1.0/Math.sqrt(2.0*Math.PI);
      step=10.0/accur;
      step1=1.0/step;
      int lim=accur-1;
      double factor=step*norm;
      mErfc[0]=0.5;
      double sum=0;
      mErfc[0]=0.5;
      for(int i=0; i<lim; i++){
        double x=i*step;
        //We set beginning point of an interval.
        //Now, do integrate carefully through this interval:
        double substep=step/subaccuracy;
        double subfactor=factor/subaccuracy;
        double subx=x;
        double subsum=0.0;
        for(int ii=0; ii<subaccuracy; ii++){
          subx+=substep;
          subsum+=Math.exp(-(subx*subx*0.5))*subfactor;
        }
        sum+=subsum;
        mErfc[i+1]=0.5-sum;
        //UTil.con("i"+i+" x"+x+" sum="+sum);
      }
    for(int k=lim-1; k>-1; k--){
      if(mErfc[k]<0){
        mErfc[k]=0.0; //Fix accuracy defect.
      }else{
        break;
      }
    }
    }
    
    //------------------------------------------------------------------------- 
  //Brute force (asymmetric) erfc calculation.
    //========================================================================= 
    
    public static double erfIntegrand(double x){
      return (erfc(x)-erfc(x+step))*step1;
    }
    
    public static double erfDerivative(double x){
      return Math.exp(-(x*x*0.5))*norm;
    }
}
