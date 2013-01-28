package uk.ac.manchester.snee.client.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.rits.cloning.Cloner;

import uk.ac.manchester.cs.snee.EvaluatorException;
import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.SNEECompilerException;
import uk.ac.manchester.cs.snee.SNEEController;
import uk.ac.manchester.cs.snee.SNEEDataSourceException;
import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.AgendaException;
import uk.ac.manchester.cs.snee.compiler.AgendaLengthException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.WhenSchedulerException;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.logicaloverlaynetworkgenerator.LogicalOverlayNetwork;
import uk.ac.manchester.cs.snee.metadata.CostParametersException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.schema.UnsupportedAttributeTypeException;
import uk.ac.manchester.cs.snee.metadata.source.SensorNetworkSourceMetadata;
import uk.ac.manchester.cs.snee.metadata.source.SourceDoesNotExistException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.RadioLink;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.TopologyReaderException;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;
import uk.ac.manchester.cs.snee.sncb.SNCBException;
import uk.ac.manchester.snee.client.RelibaleChannelClient;

public class rewriteTopologies
{
  
  private static File testFolder =  new File("src/main/resources/testsSize30");
  private static File sneetestFolder =  new File("testsSize30");
  private static int queryid = 1;
  private static String sep = System.getProperty("file.separator");
  
  public static void main(String[] args) 
  {
    //This method represents the web server wrapper
    // Configure logging
  //  PropertyConfigurator.configure(
      //  SuccessorClient.class.
      //  getClassLoader().getResource("etc/common/log4j.properties"));
    
    
    Long duration = Long.valueOf("120");
    String queryParams = "etc/query-parameters.xml";
    Iterator<String> queryIterator;
    try
    {
      //holds all 30 queries produced by python script.
      ArrayList<String> queries = new ArrayList<String>();
      collectQueries(queries);
      
      queryIterator = queries.iterator();
      //TODO remove to allow full run
      for(int index = 1; index < 62; index++)
      {
        queryIterator.next();  
        queryid++;
      }
      
      while(queryIterator.hasNext())
      {
        rewrite(queryIterator, duration, queryParams, true);
        queryid++;
      }
    }
    catch (Exception e)
    {
      System.out.println("Execution failed. See logs for detail.");
      System.out.println("error message was " + e.getMessage());
      e.printStackTrace();
    }
  }
  
  /**
   * generates a basic QEP from the topolgoy, then starts the rewriting of the topology
   * @param queryIterator
   * @param duration
   * @param queryParams
   * @param b
   * @param newKLevel 
   * @throws SNEEConfigurationException 
   * @throws IOException 
   * @throws SNEEException 
   * @throws AgendaLengthException 
   * @throws WhenSchedulerException 
   * @throws CodeGenerationException 
   * @throws SNCBException 
   * @throws CostParametersException 
   * @throws SNEEDataSourceException 
   * @throws TopologyReaderException 
   * @throws SourceMetadataException 
   * @throws UnsupportedAttributeTypeException 
   * @throws AgendaException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws MetadataException 
   * @throws EvaluatorException 
   * @throws SNEECompilerException 
   * @throws NumberFormatException 
   */
  private static boolean rewrite(Iterator<String> queryIterator, Long duration,
      String queryParams, boolean b)
  {
    try
    {
      String currentQuery = queryIterator.next();
      String propertiesPath = sneetestFolder.toString() + sep + "snee" + queryid + ".properties";
      System.out.println("rewriting topology for query " + (queryid));
      for(int newKLevel = 2; newKLevel <= 5; newKLevel++)
      {
        RelibaleChannelClient snee = new RelibaleChannelClient(currentQuery, duration, queryParams, null, propertiesPath);
        SNEEController contol = (SNEEController) snee.getController();
        contol.setQueryID(queryid);
        runCompilelation(contol, queryParams, currentQuery);
        SensorNetworkQueryPlan qep = snee.getQEP();
        rewriteTopology(qep, newKLevel, snee);
      }
      return true;
      
    }
    catch(Exception e)
    {
      e.printStackTrace();
      return false;
    }
  }

  /**
   * takes the generated qep and rt, and rebuilds a topolgoy layout to add extra nodes and links to work with 
   * unreliable channel code
   * @param qep
   * @throws SourceDoesNotExistException 
   * @throws IOException 
   */
  private static void rewriteTopology(SensorNetworkQueryPlan qep, 
                                      int newKLevel,  
                                      RelibaleChannelClient snee ) 
  throws SourceDoesNotExistException, IOException
  {
    Cloner cloner = new Cloner();
    cloner.dontClone(Logger.class);
    RT rt = qep.getRT();
    RT cloned = cloner.deepClone(rt);
    int maxID = rt.getMaxSiteID();
    int counter = 1;
    LogicalOverlayNetwork overlay = new LogicalOverlayNetwork();
    Topology network = getWsnTopology(snee);
    HashMap<String, Site> sites = new HashMap<String, Site>();
    Iterator<Site> siteIterator = rt.siteIterator(TraversalOrder.POST_ORDER);
    while(siteIterator.hasNext())
    {
      Site site = siteIterator.next();
      sites.put(site.getID(), site);
      ArrayList<String> eqSites = new ArrayList<String>();
      for(int index = 0; index < newKLevel; index++)
      {
        Site eqSite = new Site(new String (new Integer(maxID + counter).toString()));
        eqSites.add(eqSite.toString());
        sites.put(eqSite.getID(), eqSite);
        counter++;
      }
      overlay.addClusterNode(site.getID(), eqSites);
    }
    //add links
    siteIterator = rt.siteIterator(TraversalOrder.POST_ORDER);
    Site tempSite = siteIterator.next();
    siteIterator = rt.siteIterator(TraversalOrder.POST_ORDER);
    while(siteIterator.hasNext())
    {
      Site site = siteIterator.next();
      Site clusterHead = sites.get(site.getID());
      
      Iterator<String> eqSiteIDs = overlay.getEquivilentNodes(clusterHead.getID()).iterator();
      while(eqSiteIDs.hasNext())
      {
        Site eqSite = sites.get(eqSiteIDs.next());
        Iterator<Node> nodeIterator = clusterHead.getInputsList().iterator();
        while(nodeIterator.hasNext())
        {
          Node input = nodeIterator.next();
          eqSite.addInput(input);
        }
        nodeIterator = clusterHead.getOutputsList().iterator();
        while(nodeIterator.hasNext())
        {
          Node input = nodeIterator.next();
          eqSite.addOutput(input);
        }
        if(!clusterHead.getID().equals(rt.getRoot().getID()))
        {
          Iterator<String> outputs = overlay.getEquivilentNodes(clusterHead.getOutputsList().get(0).toString()).iterator();
          while(outputs.hasNext())
          {
            Node output = sites.get(outputs.next());
            eqSite.addOutput(output);
            site.addOutput(output);
          }
        }
        site.addInput(eqSite);
      }
    }
    
    //generated all nodes, need to generate topology file
    BufferedWriter out = new BufferedWriter(new FileWriter(
        new File("output" + sep + "query" + queryid + sep + "top" + queryid + "." + newKLevel +".xml")));
    
    out.write("<?xml version=\"1.0\"?> \n \n <network-topology \n xmlns=\"http://snee.cs.manchester.ac.uk\""+ 
        "\n xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" \n xsi:schemaLocation=\"http://snee.cs.manchester.ac.uk network-topology.xsd\">" +
        "\n\n <units> \n  <energy>MILLIJOULES</energy> \n <memory>BYTES</memory> \n  <time>MILLISECONDS</time>" + 
        "</units> \n\n  <radio-links> \n");
    
    Iterator<String> keys = sites.keySet().iterator();
    ArrayList<String> doneEdges = new ArrayList<String>();
    while(keys.hasNext())
    {
      Site site = sites.get(keys.next());
      RadioLink linkBetweenClusters = network.getRadioLink(tempSite, (Site)tempSite.getOutput(0));
      Iterator<Node> inputIterator = site.getInputsList().iterator();
      while(inputIterator.hasNext())
      {
        Node input = inputIterator.next();
        
        if((input.getID().equals("2") && site.getID().equals("36")) ||
            site.getID().equals("2") && input.getID().equals("36"))
          System.out.println();
        RadioLink link = network.getRadioLink(site, (Site)input);
        if(link != null)
        {
          if(cloned.getRadioLink(site, (Site)input) != null)
          {
            if(!doneEdges.contains(new String(site.getID() + ":" + input.getID())))
            {
              out.write("<radio-link source=\"" + site.getID() + "\" dest=\"" +
                        input + "\" bidirectional=\"true\" energy=\"" + link.getEnergyCost() + 
                        "\" time=\"" + link.getDistanceCost() + "\" radio-loss=\"0\"/> \n");
              doneEdges.add(site.getID() + ":" + input.getID());
              doneEdges.add(input.getID() + ":" + site.getID());
            }
            if(!overlay.getEquivilentNodes(site.getID()).contains(input.getID()))
              linkBetweenClusters = link;
          }
          else
          {
            if(!doneEdges.contains(new String(site.getID() + ":" + input.getID())))
            {
              Double energyCost = link.getEnergyCost() * 6;
              out.write("<radio-link source=\"" + site.getID() + "\" dest=\"" +
                  input + "\" bidirectional=\"true\" energy=\"" + energyCost + 
                  "\" time=\"" + link.getDistanceCost() + "\" radio-loss=\"0\"/> \n");
              doneEdges.add(site.getID() + ":" + input.getID());
              doneEdges.add(input.getID() + ":" + site.getID());
            }
           if(!overlay.getEquivilentNodes(site.getID()).contains(input.getID()))
             linkBetweenClusters = link;
          }
        }
        else
        {
          if(!overlay.getEquivilentNodes(site.getID()).contains(input.getID()))
          {
            Double energyCost = linkBetweenClusters.getEnergyCost() * 6;
            if(energyCost > 254)
              energyCost = new Double(254);
            if(!doneEdges.contains(new String(site.getID() + ":" + input.getID())))
            {
              //internal cluster link.
              out.write("<radio-link source=\"" + site.getID() + "\" dest=\"" +
                  input + "\" bidirectional=\"true\" energy=\"" + energyCost + 
                  "\" time=\"" + linkBetweenClusters.getDistanceCost() / 10 + "\" radio-loss=\"0\"/> \n");
              doneEdges.add(site.getID() + ":" + input.getID());
              doneEdges.add(input.getID() + ":" + site.getID());
            }
          }
          else
          {
          //external cluster link.
            if(cloned.getRadioLink(site, (Site)input) != null)
            {
              if(!doneEdges.contains(new String(site.getID() + ":" + input.getID())))
              {
                out.write("<radio-link source=\"" + site.getID() + "\" dest=\"" +
                  input + "\" bidirectional=\"true\" energy=\"" + linkBetweenClusters.getEnergyCost() + 
                  "\" time=\"" + linkBetweenClusters.getDistanceCost() /2 + "\" radio-loss=\"0\"/> \n");
                doneEdges.add(site.getID() + ":" + input.getID());
                doneEdges.add(input.getID() + ":" + site.getID());
              }
            }
            else
            {
              if(!doneEdges.contains(new String(site.getID() + ":" + input.getID())))
              {
                Double energyCost = linkBetweenClusters.getEnergyCost() * 6;
                if(energyCost > 254)
                  energyCost = new Double(254);
                out.write("<radio-link source=\"" + site.getID() + "\" dest=\"" +
                    input + "\" bidirectional=\"true\" energy=\"" + energyCost  + 
                    "\" time=\"" + linkBetweenClusters.getDistanceCost()  + "\" radio-loss=\"0\"/> \n");
                doneEdges.add(site.getID() + ":" + input.getID());
                doneEdges.add(input.getID() + ":" + site.getID());
              }
            }
          }
        }
        if(!doneEdges.contains(new String(site.getID() + ":" + input.getID())))
        {
          doneEdges.add(site.getID() + ":" + input.getID());
          doneEdges.add(input.getID() + ":" + site.getID());
        }
      }
      
      Iterator<Node> outputIterator = site.getOutputsList().iterator();
      while(outputIterator.hasNext())
      {
        Node output = outputIterator.next();
        if((output.getID().equals("2") && site.getID().equals("36")) ||
            site.getID().equals("2") && output.getID().equals("36"))
          System.out.println();
        RadioLink link = network.getRadioLink(site, (Site)output);
        if(link != null)
        {
          if(cloned.getRadioLink(site, (Site)output) != null)
          {
            if(!doneEdges.contains(new String(site.getID() + ":" + output.getID())))
            {
              out.write("<radio-link source=\"" + site.getID() + "\" dest=\"" +
                        output + "\" bidirectional=\"true\" energy=\"" + link.getEnergyCost() + 
                        "\" time=\"" + link.getDistanceCost() + "\" radio-loss=\"0\"/> \n");
              doneEdges.add(site.getID() + ":" + output.getID());
              doneEdges.add(output.getID() + ":" + site.getID());
            }
            if(!overlay.getEquivilentNodes(site.getID()).contains(output.getID()))
              linkBetweenClusters = link;
          }
          else
          {
            Double energyCost = link.getEnergyCost() * 6;
            if(energyCost > 254)
              energyCost = new Double(254);
            if(!doneEdges.contains(new String(site.getID() + ":" + output.getID())))
            {
              out.write("<radio-link source=\"" + site.getID() + "\" dest=\"" +
                  output + "\" bidirectional=\"true\" energy=\"" +energyCost  + 
                  "\" time=\"" + link.getDistanceCost() + "\" radio-loss=\"0\"/> \n");
              doneEdges.add(site.getID() + ":" + output.getID());
              doneEdges.add(output.getID() + ":" + site.getID());
            }
            
            if(!overlay.getEquivilentNodes(site.getID()).contains(output.getID()))
              linkBetweenClusters = link;
          }
        }
        else
        {
          if(!overlay.getEquivilentNodes(site.getID()).contains(output.getID()))
          {
            //internal cluster link.
            Double energyCost = linkBetweenClusters.getEnergyCost() * 6;
            if(energyCost > 254)
              energyCost = new Double(254);
            if(!doneEdges.contains(new String(site.getID() + ":" + output.getID())))
            {
              out.write("<radio-link source=\"" + site.getID() + "\" dest=\"" +
                  output + "\" bidirectional=\"true\" energy=\"" + energyCost + 
                  "\" time=\"" + linkBetweenClusters.getDistanceCost() / 10 + "\" radio-loss=\"0\"/> \n");
              doneEdges.add(site.getID() + ":" + output.getID());
              doneEdges.add(output.getID() + ":" + site.getID());
            }
          }
          else
          {
          //external cluster link.
            if(cloned.getRadioLink(site, (Site)output) != null)
            {
              if(!doneEdges.contains(new String(site.getID() + ":" + output.getID())))
              {
                out.write("<radio-link source=\"" + site.getID() + "\" dest=\"" +
                    output + "\" bidirectional=\"true\" energy=\"" + linkBetweenClusters.getEnergyCost() + 
                    "\" time=\"" + linkBetweenClusters.getDistanceCost()  + "\" radio-loss=\"0\"/> \n");
                doneEdges.add(site.getID() + ":" + output.getID());
                doneEdges.add(output.getID() + ":" + site.getID());
            }
            }
            else
            {
              Double energyCost = linkBetweenClusters.getEnergyCost() * 6;
              if(energyCost > 254)
                energyCost = new Double(254);
              if(!doneEdges.contains(new String(site.getID() + ":" + output.getID())))
              {
                out.write("<radio-link source=\"" + site.getID() + "\" dest=\"" +
                    output + "\" bidirectional=\"true\" energy=\"" + energyCost + 
                    "\" time=\"" + linkBetweenClusters.getDistanceCost()  + "\" radio-loss=\"0\"/> \n");
                doneEdges.add(site.getID() + ":" + output.getID());
                doneEdges.add(output.getID() + ":" + site.getID());
              } 
            }
          }
        }
        if(!doneEdges.contains(new String(site.getID() + ":" + output.getID())))
        {
          doneEdges.add(site.getID() + ":" + output.getID());
          doneEdges.add(output.getID() + ":" + site.getID());
        }
      }
    }
    out.write("\n\n </radio-links> \n \n </network-topology> \n");
    out.flush();
    out.close();
  }
  
  /**
   * helper method to get topology from the qep
   * @param snee 
   * @return topology
   * @throws SourceDoesNotExistException 
   */
  public static Topology getWsnTopology(RelibaleChannelClient snee) 
  throws SourceDoesNotExistException
  {
    SensorNetworkSourceMetadata metadata = (SensorNetworkSourceMetadata) snee.getMetadata();
    Topology network = metadata.getTopology();
    return network;
  }
  

  /**
   * generates a simple QEP
   * @param contol
   * @param queryParams
   * @param currentQuery
   * @throws SNEECompilerException
   * @throws MalformedURLException
   * @throws EvaluatorException
   * @throws SNEEException
   * @throws MetadataException
   * @throws SNEEConfigurationException
   * @throws OptimizationException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws AgendaException
   * @throws UnsupportedAttributeTypeException
   * @throws SourceMetadataException
   * @throws TopologyReaderException
   * @throws SNEEDataSourceException
   * @throws CostParametersException
   * @throws SNCBException
   * @throws IOException
   * @throws CodeGenerationException
   * @throws NumberFormatException
   * @throws WhenSchedulerException
   * @throws AgendaLengthException
   */
  public static void runCompilelation(SNEEController contol, String queryParams, String currentQuery) 
  throws 
  SNEECompilerException, MalformedURLException, 
  EvaluatorException, SNEEException, MetadataException, 
  SNEEConfigurationException, OptimizationException, 
  SchemaMetadataException, TypeMappingException, AgendaException, 
  UnsupportedAttributeTypeException, SourceMetadataException, 
  TopologyReaderException, SNEEDataSourceException, 
  CostParametersException, SNCBException, IOException, 
  CodeGenerationException, NumberFormatException, WhenSchedulerException,
  AgendaLengthException 
  {
    SNEEProperties.setSetting(SNEEPropertyNames.WSN_MANAGER_SUCCESSOR, "FALSE");
    SNEEProperties.setSetting(SNEEPropertyNames.RUN_SIM_FAILED_NODES, "FALSE");
    SNEEProperties.setSetting(SNEEPropertyNames.RUN_AVRORA_SIMULATOR, "FALSE");
    SNEEProperties.setSetting(SNEEPropertyNames.RUN_AVRORA_SIMULATOR, "FALSE");
    SNEEProperties.setSetting(SNEEPropertyNames.WSN_MANAGER_INITILISE_FRAMEWORKS, "FALSE");
    SNEEProperties.setSetting(SNEEPropertyNames.WSN_MANAGER_UNRELIABLE_CHANNELS, "FALSE");
    
    contol.addQuery(currentQuery, queryParams);
    contol.close();
  }
  
  /**
   * 
   * @param queries
   * @throws IOException
   */
  private static void collectQueries(ArrayList<String> queries) throws IOException
  {
     //String filePath = Utils.validateFileLocation("tests/queries.txt");
     File queriesFile = new File(testFolder.toString() + sep + "queries.txt");
     String filePath = queriesFile.getAbsolutePath();
     BufferedReader queryReader = new BufferedReader(new FileReader(filePath));
     String line = "";
     int counter = 0;
     while((line = queryReader.readLine()) != null)
     {
       if(counter >= queryid)
         queries.add(line);
       else
         counter++;
     }  
  }
  
  
  
  
}
