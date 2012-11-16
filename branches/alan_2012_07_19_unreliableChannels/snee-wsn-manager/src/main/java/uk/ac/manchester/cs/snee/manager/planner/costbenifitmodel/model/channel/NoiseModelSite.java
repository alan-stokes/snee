package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.channel;
//class ported from Hyungjune lee from stanford. 
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Random;

public class NoiseModelSite
{
  private ArrayList<Integer> key = new ArrayList<Integer>(NoiseModelConstants.NOISE_HISTORY + 1);
  private ArrayList<Integer> freqKey = new ArrayList<Integer>();
  private Integer lastNoiseVal;
  private int lastTimeNoiseGenerated; // preivous time
  private Hashtable<String, NoiseHash> noiseTable = new Hashtable<String, NoiseHash>();
  private ArrayList<Integer> localNoiseTrace = new ArrayList<Integer>(NoiseModelConstants.NOISE_MIN_TRACE);
  private int noiseTraceLen;
  private int noiseTraceIndex = 0;
  private boolean hasBeenGenerated;
  
  private static Random random = new Random(new Long(0));
  private static ArrayList<Integer> globalNoiseTrace = new ArrayList<Integer>(NoiseModelConstants.NOISE_MIN_TRACE);
  private static Integer globalNoiseTraceIndex = 0;
  private static int globalNoiseTraceLen = NoiseModelConstants.NOISE_MIN_TRACE;
  /**
   * constructor
   */
  public NoiseModelSite()
  {
    for(int index = 0; index < NoiseModelConstants.NOISE_MIN_TRACE; index++)
    {
      Integer newInt = random.nextInt(127);
      newInt -= 127;
      localNoiseTrace.add(index, newInt);
    }
    for(int index = 0; index < NoiseModelConstants.NOISE_HISTORY; index++)
    {
      Integer newInt = random.nextInt(127);
      newInt -= 127;
      if(newInt > NoiseModelConstants.NOISE_MAX || newInt < NoiseModelConstants.NOISE_MIN)
        newInt = NoiseModelConstants.NOISE_MIN;
      newInt = (newInt - NoiseModelConstants.NOISE_MIN) / NoiseModelConstants.NOISE_QUANTIZE_INTERVAL + 1;
      this.key.add(index, newInt);
    }
    noiseTraceLen = NoiseModelConstants.NOISE_MIN_TRACE;
  }
  
  /**
   * basic getter and setters
   *
   */
  public ArrayList<Integer> getKey()
  {
    return key;
  }

  public void setKey(ArrayList<Integer> key)
  {
    ArrayList<Integer> temp = new ArrayList<Integer>(key);
    this.key.clear();
    this.key.addAll(temp);
  }

  public ArrayList<Integer> getFreqKey()
  {
    return freqKey;
  }

  public void setFreqKey(ArrayList<Integer> freqKey)
  {
    ArrayList<Integer> temp = new ArrayList<Integer>(freqKey);
    this.freqKey.clear();
    this.freqKey.addAll(temp);
  }

  public Integer getLastNoiseVal()
  {
    return lastNoiseVal;
  }

  public void setLastNoiseVal(Integer lastNoiseVal)
  {
    this.lastNoiseVal = lastNoiseVal;
  }

  public int getLastTimeNoiseGenerated()
  {
    return lastTimeNoiseGenerated;
  }

  public void setLastTimeNoiseGenerated(int timeNoiseGenerated)
  {
    this.lastTimeNoiseGenerated = timeNoiseGenerated;
  }

  public Hashtable<String, NoiseHash> getNoiseTable()
  {
    return noiseTable;
  }

  public void setNoiseTable(Hashtable<String, NoiseHash> noiseTable)
  {
    this.noiseTable = noiseTable;
  }

  public ArrayList<Integer> getNoiseTrace()
  {
    return this.localNoiseTrace;
  }
  
  public static ArrayList<Integer> getGlobalNoiseTrace()
  {
    return globalNoiseTrace;
  }
  
  

  public int getNoiseTraceLen()
  {
    return noiseTraceLen;
  }
  
  public static int getGlobalNoiseTraceLen()
  {
    return globalNoiseTraceLen;
  }
  
  public static void setGlobalNoiseTraceLen(int noiseTraceLen)
  {
    globalNoiseTraceLen = noiseTraceLen;
  }
  
  public void setNoiseTraceLen(int noiseTraceLen)
  {
    this.noiseTraceLen = noiseTraceLen;
  }

  public int getNoiseTraceIndex()
  {
    return noiseTraceIndex;
  }
  
  public static int getGlobalNoiseTraceIndex()
  {
    return globalNoiseTraceIndex;
  }
  
  public static void setGlobalNoiseTraceIndex(int noiseTraceIndex)
  {
    globalNoiseTraceIndex = noiseTraceIndex;
  }
  

  public void setNoiseTraceIndex(int noiseTraceIndex)
  {
    this.noiseTraceIndex = noiseTraceIndex;
  }

  public boolean isHasBeenGenerated()
  {
    return hasBeenGenerated;
  }

  public void setHasBeenGenerated(boolean hasBeenGenerated)
  {
    this.hasBeenGenerated = hasBeenGenerated;
  }

  public String getKeyStringForm()
  {
    Iterator<Integer> keyIterator = this.key.iterator();
    String output = "";
    while(keyIterator.hasNext())
    {
      Integer value = keyIterator.next();
      output = output.concat(value.toString() + ",");
      if(output.length() > 100)
        System.out.println();
    }
    return output;
  }

  public String getFreqKeyStringForm()
  {
    Iterator<Integer> keyIterator = this.freqKey.iterator();
    String output = "";
    while(keyIterator.hasNext())
    {
      Integer value = keyIterator.next();
      output = output.concat(value.toString() + ",");
    }
    return output;
  }

  public void setFreqKey(String freqInStringForm)
  {
    ArrayList<Integer> fre = new ArrayList<Integer>();
    String [] bits = freqInStringForm.split(",");
    for(int index = 0; index < bits.length; index++)
    {
      fre.add(Integer.parseInt(bits[index]));
    }
    this.setFreqKey(fre);
  }
}

