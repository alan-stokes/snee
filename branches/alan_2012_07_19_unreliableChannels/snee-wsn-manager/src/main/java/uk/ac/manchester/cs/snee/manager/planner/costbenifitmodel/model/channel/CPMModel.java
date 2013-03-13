package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model.channel;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Random;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;

public class CPMModel
{
  private Topology network;
  private int frequencyKeyNum = 0;
  private static Random random;
  //Containers for noise model stuff
  private ArrayList<Integer> experimentalNoiseMeasurements = new ArrayList<Integer>();
  private HashMap<String, NoiseModelSite> noiseModelSites = new HashMap<String, NoiseModelSite>();;
  private static Hashtable<String, NoiseHash> globalNoiseTable = new Hashtable<String, NoiseHash>();
  private boolean onFirstNode = true;
  
  public CPMModel(Topology network)
  throws SNEEConfigurationException, IOException
  {
    this.network = network;
    readinNoiseMeasurements();
    makeNoiseModelNodes();
    addGlobalNoiseTrace();
    setUpCPMModel();
    random = new Random(new Long(0));
  }
  
  public CPMModel(Topology network, Long seed)
  throws SNEEConfigurationException, IOException
  {
    this.network = network;
    readinNoiseMeasurements();
    makeNoiseModelNodes();
    addGlobalNoiseTrace();
    setUpCPMModel();
    random = new Random(seed);
  }

  private void addGlobalNoiseTrace()
  {
    Iterator<Integer> experimentalNoiseIterator = this.experimentalNoiseMeasurements.iterator();
    while(experimentalNoiseIterator.hasNext())
    {
      Integer noiseValue = experimentalNoiseIterator.next();
      
      if(NoiseModelSite.getGlobalNoiseTraceIndex() == NoiseModelSite.getGlobalNoiseTraceLen())
      {
        NoiseModelSite.setGlobalNoiseTraceLen(NoiseModelSite.getGlobalNoiseTraceLen() * 2);
      }
      NoiseModelSite.getGlobalNoiseTrace().add(NoiseModelSite.getGlobalNoiseTraceIndex(), noiseValue);
      NoiseModelSite.setGlobalNoiseTraceIndex(NoiseModelSite.getGlobalNoiseTraceIndex() + 1);
    }
  }

  /**
   * set up CPM model
   */
  private void setUpCPMModel()
  {
    Iterator<String> nodeKeyIterator = this.network.getAllNodes().keySet().iterator();
    ArrayList<String> nodeIds = new ArrayList<String>();
    for(int index =0; index <= this.network.getMaxNodeID(); index++)
    {
      nodeIds.add(null);
    }
    while(nodeKeyIterator.hasNext())
    {
      String id = nodeKeyIterator.next();
      nodeIds.set(Integer.parseInt(id), id);
    }
    nodeKeyIterator = nodeIds.iterator();
    //add nodes and experimental noise
    while(nodeKeyIterator.hasNext())
    {
      String nodeKey = nodeKeyIterator.next();
      if(nodeKey != null)
      {
        System.out.println("setting up for node " + nodeKey);
        NoiseModelSite noiseNode =  noiseModelSites.get(nodeKey);
        createNoiseModel(noiseNode);
        resetNode(noiseNode);
      }
    }  
  }

  /**
   * allows the simulation to start from beginning of noise model
   * @param noiseNode
   */
  private void resetNode(NoiseModelSite noiseNode)
  {
    for(int index = 0; index < NoiseModelConstants.NOISE_HISTORY; index++)
    {
      ArrayList<Integer> key = noiseNode.getKey();
      Integer keyOfIndex = noiseNode.getNoiseTrace().get(index);
      key.set(index, search_bin_num(keyOfIndex));
      noiseNode.setKey(key);
    } 
  }

  /**
   * entrance method for generating noise model
   */
  private void makeNoiseModelNodes()
  {
    Iterator<String> nodeKeyIterator = this.network.getAllNodes().keySet().iterator();
    ArrayList<String> nodeIds = new ArrayList<String>();
    for(int index =0; index <= this.network.getMaxNodeID(); index++)
    {
      nodeIds.add(null);
    }
    while(nodeKeyIterator.hasNext())
    {
      String id = nodeKeyIterator.next();
      nodeIds.set(Integer.parseInt(id), id);
    }
    nodeKeyIterator = nodeIds.iterator();
    //add nodes and experimental noise
    while(nodeKeyIterator.hasNext())
    {
      String nodeKey = nodeKeyIterator.next();
      if(nodeKey != null)
      {
        Node node = this.network.getAllNodes().get(nodeKey);
        NoiseModelSite noiseNode = new NoiseModelSite();
        noiseModelSites.put(node.getID(), noiseNode);
      }
    }
  }
  
  /**
   * method to create the cpm model
   */
  private void createNoiseModel(NoiseModelSite noiseNode)
  {
    makeNoiseModel(noiseNode);
    makePmfDistr(noiseNode);
  }
  
  /**
   * entrance method to generating pmf for nodes hash tables.
   * @param noiseNode
   */
  private void makePmfDistr(NoiseModelSite noiseNode)
  {
    ArrayList<Integer> nodeKey = noiseNode.getKey();
    this.frequencyKeyNum = 0;
    for(int index = 0; index < NoiseModelConstants.NOISE_HISTORY; index++)
    {
      nodeKey.set(index,search_bin_num(noiseNode.getNoiseTrace().get(index)));
    }
    noiseNode.setKey(nodeKey);
    
    int globalCount = 0;
    for(int index = NoiseModelConstants.NOISE_HISTORY; index < NoiseModelSite.getGlobalNoiseTraceIndex(); index++)
    {
      simNoiseDist(noiseNode);
      arrangeKey(noiseNode);
      if(globalCount < (NoiseModelConstants.NOISE_MIN_TRACE -  NoiseModelConstants.NOISE_HISTORY -1))
      {
        noiseNode.getKey().set(NoiseModelConstants.NOISE_HISTORY -1, search_bin_num(noiseNode.getNoiseTrace().get(index)));
      }
      else if(globalCount < NoiseModelConstants.NOISE_MIN_TRACE && 
              globalCount >= (NoiseModelConstants.NOISE_MIN_TRACE - NoiseModelConstants.NOISE_HISTORY -1))
      {
        noiseNode.getKey().set(NoiseModelConstants.NOISE_HISTORY - 1, 
            this.search_bin_num(NoiseModelSite.getGlobalNoiseTrace().get(20 - (index - globalCount))));
      }
      else
      {
        noiseNode.getKey().set(NoiseModelConstants.NOISE_HISTORY -1, search_bin_num(NoiseModelSite.getGlobalNoiseTrace().get(index - globalCount)));
      }
      globalCount++;
    }
  }
  
  /**
   * executes the distrubtion calculation code and locates most frequenct key
   * @param noiseNode
   */
  private void simNoiseDist(NoiseModelSite noiseNode)
  {
    Hashtable<String, NoiseHash> localnoiseTable = noiseNode.getNoiseTable();
    String key = noiseNode.getKeyStringForm();
    //search local table first
    NoiseHash localHash = localnoiseTable.get(key);
    //if local table returns null, search global table 
    NoiseHash globalHash = CPMModel.globalNoiseTable.get(key);
    if(localHash == null && globalHash != null)
      doHashStuff(noiseNode, globalHash);
    else if(localHash != null && globalHash == null)
      doHashStuff(noiseNode, localHash);
    else if(localHash != null && globalHash != null)
    {
      combineHashes(localHash, globalHash);
      doHashStuff(noiseNode, localHash);
    }
    else if(localHash == null && globalHash == null)
      System.out.println("BROKE");
  }
  
  
  /**
   * combines two hashes, one from the local store, and one from global store, 
   * puts combined store into local store, to be kept in local hash table
   * @param localHash
   * @param globalHash
   */
  private void combineHashes(NoiseHash localHash, NoiseHash globalHash)
  {
    Iterator<Integer> elementIterator = globalHash.getElements().iterator();
    while(elementIterator.hasNext())
    {
      Integer element = elementIterator.next();
      localHash.addElement(element);
    }
  }

  /**
   * does probability density function for hash and stores most frequent key into noise node
   * @param noiseNode
   * @param hash
   */
  private void doHashStuff(NoiseModelSite noiseNode, NoiseHash hash)
  {
    if(!hash.getFlag())
    {
      initiliseHashDistance(hash);
     
      //counting how often a noise value has been placed in this hash
      int bin;
      for(int index = 0; index < hash.getNumElements(); index ++)
      {
        double val;
        bin = search_bin_num(hash.getElements().get(index));
        val = hash.getDist().get(bin);
        val += 1.0;
        hash.getDist().set(bin, val);
      }
      
      double cmf = 0;
      for(int index = 0; index < NoiseModelConstants.NOISE_BIN_SIZE; index++)
      {
        hash.getDist().set(index, (hash.getDist().get(index) / hash.getNumElements()));
        cmf += hash.getDist().get(index);
        hash.getDist().set(index, cmf);
      }
      
      hash.setFlag(true);
    }
    
    //find the most frequent key and store it in the nosie node frequency key.
    if(hash.getNumElements() > this.frequencyKeyNum)
    {
      frequencyKeyNum = hash.getNumElements();
      noiseNode.setFreqKey(noiseNode.getKey());
      System.out.println(noiseNode.getKey());
    }
  }

  private void initiliseHashDistance(NoiseHash hash)
  {
    //inisatiation
    while(hash.getNumElements() >=  hash.getDist().size())
    {
      hash.getDist().add(0.0);
    }
    //inisatiation
    for(int index = 0; index < hash.getNumElements() -1; index++)
    {
      hash.getDist().set(index, 0.0);
    }
  }

  /**
   * makes more stuff for noise model
   * @param noiseNode
   */
  private void makeNoiseModel(NoiseModelSite noiseNode)
  {
    //making new key from trace made from random values.
    for(int index = 0; index < NoiseModelConstants.NOISE_HISTORY; index++)
    {
      ArrayList<Integer> key = noiseNode.getKey();
      Integer keyOfIndex = noiseNode.getNoiseTrace().get(index);
      key.set(index, search_bin_num(keyOfIndex));
      noiseNode.setKey(key);
    }
    
    addNoiseToNoiseNode(noiseNode, noiseNode.getNoiseTable(), noiseNode.getNoiseTrace().get(NoiseModelConstants.NOISE_HISTORY));
    arrangeKey(noiseNode);
    
    int globalCount = 0;
    
    for(int index = NoiseModelConstants.NOISE_HISTORY; index < NoiseModelSite.getGlobalNoiseTraceIndex(); index++)
    {
      if(globalCount < (NoiseModelConstants.NOISE_MIN_TRACE -  NoiseModelConstants.NOISE_HISTORY -1))
      {
        noiseNode.getKey().set(NoiseModelConstants.NOISE_HISTORY - 1, 
            this.search_bin_num(noiseNode.getNoiseTrace().get(index)));
        addNoiseToNoiseNode(noiseNode, noiseNode.getNoiseTable(), noiseNode.getNoiseTrace().get(index + 1));
        globalCount++;
      }
      else if(globalCount < NoiseModelConstants.NOISE_MIN_TRACE && 
              globalCount >= (NoiseModelConstants.NOISE_MIN_TRACE - NoiseModelConstants.NOISE_HISTORY -1))
      {
        noiseNode.getKey().set(NoiseModelConstants.NOISE_HISTORY - 1, 
            this.search_bin_num(NoiseModelSite.getGlobalNoiseTrace().get(20 - (index - globalCount))));
        addNoiseToNoiseNode(noiseNode, noiseNode.getNoiseTable(), NoiseModelSite.getGlobalNoiseTrace().get(20 - (index - globalCount)));
        globalCount++;
      }
      else if(this.onFirstNode)
      {
        noiseNode.getKey().set(NoiseModelConstants.NOISE_HISTORY - 1, 
            this.search_bin_num(NoiseModelSite.getGlobalNoiseTrace().get(index- globalCount)));
        addNoiseToNoiseNode(noiseNode, CPMModel.globalNoiseTable, NoiseModelSite.getGlobalNoiseTrace().get(index - globalCount));
        globalCount++;
      }
      arrangeKey(noiseNode);
    }
    noiseNode.setHasBeenGenerated(true);
    onFirstNode = false;
  }
  
  /**
   * adds the experimental noise to the nodes noise hash table
   * @param noiseNode
   */
  private void addNoiseToNoiseNode(NoiseModelSite noiseNode, Hashtable<String, NoiseHash> noiseTable, Integer noiseValue)
  {
    String noiseKey = noiseNode.getKeyStringForm();
    NoiseHash noiseHash = noiseTable.get(noiseKey);
    //if key not in hashtable. insert new one
    if(noiseHash == null)
    {
      noiseHash = new NoiseHash(noiseKey, 0);
      noiseTable.put(noiseKey, noiseHash);
    }
    noiseHash.addElement(noiseValue);  
  }
  
  /**
   * reads in noise values from a text file corrasponding to experimental assessments. 
   * @throws SNEEConfigurationException
   * @throws IOException
   */
  private void readinNoiseMeasurements() 
  throws SNEEConfigurationException, IOException
  {  
     String filePath = SNEEProperties.getSetting(SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS_NOISEMODEL);
     
     BufferedReader in = new BufferedReader(new FileReader(new File(filePath)));
     String line = null;
     while((line = in.readLine()) != null)
     {
       if(successfulConvert(line));
         experimentalNoiseMeasurements.add(Integer.parseInt(line));
     }
  }
  
  /**
   * checks if the text can be converted into a double
   * @param line
   * @return
   */
  private boolean successfulConvert(String line)
  {
    try
    {
      Integer.parseInt(line);
    }
    catch(Exception e)
    {
      return false;
    }
    return true;
  }
  
  
  //helper method for make pmfDistr
  private Integer search_bin_num(int noise)
  {
   if(noise > NoiseModelConstants.NOISE_MAX || noise <  NoiseModelConstants.NOISE_MIN)
   {
     noise = NoiseModelConstants.NOISE_MIN;
   }
   int val = ((noise - NoiseModelConstants.NOISE_MIN) / (NoiseModelConstants.NOISE_QUANTIZE_INTERVAL)) + 1;
   return val;
  }
  
  private Integer search_noise_from_bin_value(int binValue)
  {
    int val = (NoiseModelConstants.NOISE_MIN + ((binValue -1) * NoiseModelConstants.NOISE_QUANTIZE_INTERVAL));
    return val;
  }

  /**
   * returns the simulated noise level of this link at this given time period.
   * @param sourceID
   * @param destID
   * @return
   */
  public Integer getNoise(String sourceID, Long startTime)
  {
    NoiseModelSite node = this.noiseModelSites.get(sourceID);
    Integer noise = null;
    //if node's noise model not been made, return a massive nosie value.
    if(!node.isHasBeenGenerated())
      return new Integer(127);
    
    int delta;
    //get difference between last time and this time
    if(node.getLastTimeNoiseGenerated() == 0)
      delta = new Long(startTime - NoiseModelConstants.NOISE_HISTORY -1).intValue();
    else
      delta = new Long(startTime - node.getLastTimeNoiseGenerated()).intValue();
    
    //little bit of code to reduce processing time
    // should be approx 1-2k
    if(delta > 2000)
    {
    	int temp = delta /1000;
    	int addedBit = delta % 1000;
    	delta = (delta / temp) + addedBit; 		
    }
    
    // if at the same time point, use same noise value
    if(delta == 0)
      noise = node.getLastNoiseVal();
    else
    {
      Integer previousNoise = 0;
      Integer currentNoise = 0;
      for(int index = 0; index < delta; index++)
      {
        previousNoise = currentNoise;
        currentNoise = simNodeGen(node);
        arrangeKey(node);
        ArrayList<Integer> key = node.getKey();
        key.set(NoiseModelConstants.NOISE_HISTORY -1, search_bin_num(currentNoise));
        node.setKey(key);
      }
      noise = previousNoise;
      node.setLastNoiseVal(noise);
    }
    node.setLastTimeNoiseGenerated(startTime.intValue());
    return noise;
  }
  
  /**
   * helper method for getNoise
   * @param node
   * @return
   */
  private Integer simNodeGen(NoiseModelSite node)
  {
    Integer noise;
    
    Hashtable<String, NoiseHash> localNoiseTable = node.getNoiseTable();
    String noiseKey = node.getKeyStringForm();
    String frequencyKey = node.getFreqKeyStringForm();
    
    Double randomValue = random.nextDouble();
    
    //search local hash table
    NoiseHash hash = localNoiseTable.get(noiseKey);
    //if local hash table does not contain value, search global hash table
    if(hash == null)
      hash = CPMModel.globalNoiseTable.get(noiseKey);
    
    //if neither has hash locate frequency key
    if(hash == null)
    {
      noise = 0;
      noiseKey = frequencyKey;
      //search local hash table before global hash table
      hash = localNoiseTable.get(noiseKey);
      //local hashtable does not contain frequency key, search global hash table
      if(hash == null)
      {
        hash = CPMModel.globalNoiseTable.get(noiseKey);
      }
    }
   
    if(hash.getNumElements() == 1)
    {
      noise = hash.getElements().get(0);
      return noise;
    }
    
    int index = 0;
    ArrayList<Integer> possibleBins = new ArrayList<Integer>();
    
    while(index < NoiseModelConstants.NOISE_BIN_SIZE - 1)
    {
      if(index == 0)
      {
        if(randomValue <= hash.getDist().get(index))
        {
          possibleBins.add(index);
        }
      }
      else if(( hash.getDist().get(index -1) < randomValue) && (randomValue <= hash.getDist().get(index)))
      {
        possibleBins.add(index);
      }
      index++;
    }
    
    int newIndex;
    if(possibleBins.size() == 1)
      newIndex = 0;
    else
      newIndex = random.nextInt(possibleBins.size() -1);
    
    boolean stag = checkForStatnation(node.getFreqKey(), possibleBins.get(newIndex));
    
    if(stag)
    {
      newIndex = random.nextInt(NoiseModelConstants.NOISE_BIN_SIZE);
      noise = this.search_noise_from_bin_value(newIndex);
    }
    else
    {
      noise = this.search_noise_from_bin_value(possibleBins.get(newIndex));
    }
    return noise;
  }

  private boolean checkForStatnation(ArrayList<Integer> frequencyKey, Integer newIndex)
  {
    boolean stag = true;
    Iterator<Integer> freIterator = frequencyKey.iterator();
    Integer compare = freIterator.next();
    while(freIterator.hasNext() && stag)
    {
      Integer next = freIterator.next();
      if(compare != next)
        stag = false;
      compare = next;
    }
    if(!stag)
      return false;
    else
    {
      return true;
    }
  }

  /**
   * helper method for getNoise
   * shifts key by one point
   * @param node
   */
  private void arrangeKey(NoiseModelSite node)
  {
    ArrayList<Integer> key = node.getKey();
    key.remove(0);
    key.add(0);
    node.setKey(key);
  }
  
  
}
