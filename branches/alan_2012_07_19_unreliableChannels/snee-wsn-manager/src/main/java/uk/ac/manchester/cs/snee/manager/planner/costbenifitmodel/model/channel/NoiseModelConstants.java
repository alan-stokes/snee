package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.channel;
//class ported from Hyungjune lee from stanford. 
public class NoiseModelConstants
{
  public static final double C_LIGHT = 2.99792458e8;//speed of light in m
  public static final int NOISE_HISTORY = 20;//the length of emmory the cpm has for estimating new noise
  public static final double d0 = 2.00;// fixed distance used as a rep for path loss model
  public static final int NOISE_MIN = -115; //min noise possible value.
  public static final int NOISE_MAX = -5; //max noise value possible.
  public static final int NOISE_MIN_QUANTIZE = -115;
  public static final int NOISE_QUANTIZE_INTERVAL = 5;
  public static final int NOISE_BIN_SIZE = ((NOISE_MAX - NOISE_MIN) / NOISE_QUANTIZE_INTERVAL) + 1;
  public static final int NOISE_MIN_TRACE = 128; // min amount of random values already added.
  public static final int NOISE_NUM_VALUES = NOISE_MAX - NOISE_MIN + 1;
  public static final int NOISE_DEFAULT_ELEMENT_SIZE = 8;
  public static final double RSSI_DISTANCE_DEPENDENT_MEAN = 4.68;

}
