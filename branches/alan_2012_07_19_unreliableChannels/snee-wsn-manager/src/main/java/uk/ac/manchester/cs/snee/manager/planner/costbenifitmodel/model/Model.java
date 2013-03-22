package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel.model;

import java.io.File;
import java.io.IOException;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Path;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;
import uk.ac.manchester.cs.snee.sncb.SNCB;

public class Model
{
  protected static File imageGenerationFolder;
  protected static boolean  compiledAlready;
  protected static String sep = System.getProperty("file.separator");
  protected static SNCB imageGenerator = null;
  protected static MetadataManager _metadataManager;  
  protected static boolean underSpareTime;
  protected Boolean useModelForBinaries = true;
  
  protected Model(){}
  
  public Model(SNCB imageGenerator)
  {
    Model.imageGenerator = imageGenerator;
  }
  
  protected void initilise(File imageGenerationFolder, MetadataManager _metadataManager,
                           Boolean useModelForBinaries)
  {
    Model.imageGenerationFolder = imageGenerationFolder;
    Model._metadataManager = _metadataManager;
    this.useModelForBinaries = useModelForBinaries;
  }
  
  /**
   * calls the sncb to genreate the nesc code images, and then the site QEP is assessed.
   * @return
   * @throws CodeGenerationException 
   * @throws OptimizationException 
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws IOException 
   */
  protected Long calculateNumberOfPacketsForSiteQEP(Adaptation adapt, String reprogrammedSite) 
  throws 
  IOException, SchemaMetadataException, 
  TypeMappingException, OptimizationException, 
  CodeGenerationException
  {
    if(this.useModelForBinaries)
    {
      return calculatePacketsFromModel(adapt, reprogrammedSite);
    }
    else
    {   
      return calculatePacketsFromNescBinaries(adapt, reprogrammedSite);
    }
  }
  
  /**
   * calculates the number of packets based off a cost model of the binaries.
   * @param adapt
   * @param reprogrammedSite
   * @return
   */
  private Long calculatePacketsFromModel(Adaptation adapt,
      String reprogrammedSite)
  {
    return (long) 1200;
  }

  
  /**
   * calculates number of packets based off the NesC compilers binaries.
   * @param adapt
   * @param reprogrammedSite
   * @return
   * @throws IOException
   * @throws SchemaMetadataException
   * @throws TypeMappingException
   * @throws OptimizationException
   * @throws CodeGenerationException
   */
  private Long calculatePacketsFromNescBinaries(Adaptation adapt, String reprogrammedSite)
  throws 
  IOException, SchemaMetadataException, 
  TypeMappingException, OptimizationException, 
  CodeGenerationException
  {
    File adaptFolder = new File(imageGenerationFolder.toString() + sep + adapt.getOverallID());
    if(!compiledAlready)
    {
      //create folder for this adaptation 
      adaptFolder.mkdir();
      imageGenerator.generateNesCCode(adapt.getNewQep(), adaptFolder.toString() + sep, Model._metadataManager);
      imageGenerator.compileReducedNesCCode(adaptFolder.toString()+ sep, adapt.getReprogrammingSites());
      compiledAlready = true;
    }
    Site site = adapt.getNewQep().getRT().getSite(reprogrammedSite);
    if(site == null)
    {
      return (long) 0;
    }
    else
    {
      File moteQEP = new File(adaptFolder.toString() + sep + "avrora_micaz_t2" + sep + "mote" + reprogrammedSite + ".elf");
      Long fileSize = new Long(0);
      if(moteQEP.exists())
        fileSize = moteQEP.length();
      else
      {
        fileSize = (long) 0; //needs to be changed back to error. results in issue with joins where join is on sink for code generator
        //throw new IOException("cant find image");
      }
      CostParameters parameters = _metadataManager.getCostParameters();
      int packetSize = parameters.getDeliverPayloadSize();
      Long packets = fileSize / packetSize;
      return packets;
    }
  }
  
  /**
   * Calculates how many hops are needed to get data from the sink to the reprogrammed node
   * @param adapt
   * @param site
   * @return
   */
  protected int calculateNumberOfHops(Adaptation adapt, String site, boolean deactivatedNodesChecking)
  {
    RT routingTree = null;
    if(!deactivatedNodesChecking)
      routingTree = adapt.getNewQep().getRT();
    else
      routingTree = adapt.getOldQep().getRT();
    
    Site sink = routingTree.getRoot();
    //checking not jumping unpon itself
    if(sink.getID().equals(site))
      return 0;
    //find path between the two nodes
    Path path = routingTree.getPath(site, sink.getID());
    //if no path return 0
    if(path.getNodes().length == 1)
      return 0;
    else
      return path.getNodes().length -1;
  }
  
  public static void setCompiledAlready(boolean compliedAlready)
  {
    Model.compiledAlready = compliedAlready;
  }
}
