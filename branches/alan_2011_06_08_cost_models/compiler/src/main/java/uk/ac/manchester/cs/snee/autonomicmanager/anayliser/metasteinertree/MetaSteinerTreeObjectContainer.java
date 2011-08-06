package uk.ac.manchester.cs.snee.autonomicmanager.anayliser.metasteinertree;

import uk.ac.manchester.cs.snee.common.graph.Tree;

public class MetaSteinerTreeObjectContainer
{

  private Tree steinerTree = null;
  private String childID = "";
  private String parentID = "";
  
  
  public MetaSteinerTreeObjectContainer(Tree steinerTree, String childID, String parentID)
  {
    this.steinerTree = steinerTree;
    this.childID = childID;
    this.parentID = parentID;
  }

  public MetaSteinerTreeObjectContainer(Tree steinerTree)
  {
    this.steinerTree = steinerTree;
  }
  
  public Tree getSteinerTree()
  {
    return steinerTree;
  }


  public void setSteinerTree(Tree steinerTree)
  {
    this.steinerTree = steinerTree;
  }


  public String getChildID()
  {
    return childID;
  }


  public void setChildID(String childID)
  {
    this.childID = childID;
  }


  public String getParentID()
  {
    return parentID;
  }


  public void setParentID(String parentID)
  {
    this.parentID = parentID;
  }
  
  
  
}
