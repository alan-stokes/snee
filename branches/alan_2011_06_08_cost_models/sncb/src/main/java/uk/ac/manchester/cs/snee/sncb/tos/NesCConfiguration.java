/****************************************************************************\ 
*                                                                            *
*  SNEE (Sensor NEtwork Engine)                                              *
*  http://code.google.com/p/snee                                             *
*  Release 1.0, 24 May 2009, under New BSD License.                          *
*                                                                            *
*  Copyright (c) 2009, University of Manchester                              *
*  All rights reserved.                                                      *
*                                                                            *
*  Redistribution and use in source and binary forms, with or without        *
*  modification, are permitted provided that the following conditions are    *
*  met: Redistributions of source code must retain the above copyright       *
*  notice, this list of conditions and the following disclaimer.             *
*  Redistributions in binary form must reproduce the above copyright notice, *
*  this list of conditions and the following disclaimer in the documentation *
*  and/or other materials provided with the distribution.                    *
*  Neither the name of the University of Manchester nor the names of its     *
*  contributors may be used to endorse or promote products derived from this *
*  software without specific prior written permission.                       *
*                                                                            *
*  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS   *
*  IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, *
*  THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR    *
*  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR          *
*  CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,     *
*  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,       *
*  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR        *
*  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF    *
*  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING      *
*  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS        *
*  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.              *
*                                                                            *
\****************************************************************************/
package uk.ac.manchester.cs.snee.sncb.tos;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.common.graph.Edge;
import uk.ac.manchester.cs.snee.common.graph.Graph;
import uk.ac.manchester.cs.snee.common.graph.Node;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;

public class NesCConfiguration extends Graph {
	
    /**
     * 	The site that this configuration pertains to, null if N/A 
     * 
     * */
    private Site site = null;
    
    boolean tossimFlag;
    
    public NesCConfiguration(final String name, final SensorNetworkQueryPlan qp, 
    		boolean tossimFlag) {
    	this(name, qp, null, tossimFlag);
    }

    public NesCConfiguration(final String name, final SensorNetworkQueryPlan qp,
	    final Site site, boolean tossimFlag) {

    	super(name, true, true);
		this.addComponent(new OutsideWorldComponent(this, tossimFlag)); //use for wiring components to the outside world
		this.site = site;
		this.tossimFlag = tossimFlag;

    }

    public Site getSite() {
	return this.site;
    }

    public String getSiteID() {
	if (this.site != null) {
	    return this.site.getID();
	} else {
	    return "null";
	}
    }

    public NesCComponent addComponent(final NesCComponent newComponent) {
	return (NesCComponent) super.addNode(newComponent);
    }

    public NesCComponent getComponent(final String name) {
	return (NesCComponent) this.getNode(name);
    }

    @Override
    public Wiring edgeFactory(final String id, final String sourceID,
	    final String destID) {
	return new Wiring(id, sourceID, destID);
    }

    public Iterator<NesCComponent> componentIterator() {
	final Collection<Node> compColl = this.nodes.values();
	final ArrayList<NesCComponent> compList = new ArrayList<NesCComponent>();

	final Iterator<Node> compCollIter = compColl.iterator();
	while (compCollIter.hasNext()) {
	    final NesCComponent comp = (NesCComponent) compCollIter.next();
	    compList.add(comp);
	}

	return compList.iterator();
    }

    private void addInAlphabeticalOrder(final ArrayList<Wiring> wirings,
	    final Wiring w) {
	int index = 0;
	final Iterator<Wiring> wiringsIter = wirings.iterator();
	while (wiringsIter.hasNext()) {
	    if (wiringsIter.next().getID().compareTo(w.getID()) > 0) {
		break;
	    }
	    index++;
	}
	wirings.add(index, w);
    }

    /**
     * Provides an iterator with the wirings for the desired user component
     * @param desiredUser
     * @return
     */
    public Iterator<Wiring> wiringsIteratorForUser(final String desiredUser) {
	final ArrayList<Wiring> wirings = new ArrayList<Wiring>();

	final Iterator<Edge> edgeIter = this.edges.values().iterator();
	while (edgeIter.hasNext()) {
	    final Wiring w = (Wiring) edgeIter.next();

	    if (w.getUser().equals(desiredUser)) {
		this.addInAlphabeticalOrder(wirings, w);
	    }
	}
	return wirings.iterator();
    }

    public Iterator<Wiring> wiringsIteratorForProvider(
	    final String desiredProvider) {
	final ArrayList<Wiring> wirings = new ArrayList<Wiring>();

	final Iterator<Edge> edgeIter = this.edges.values().iterator();
	while (edgeIter.hasNext()) {
	    final Wiring w = (Wiring) edgeIter.next();

	    if (w.getProvider().equals(desiredProvider)) {
		this.addInAlphabeticalOrder(wirings, w);
	    }
	}

	return wirings.iterator();
    }

    /**
     * Provides an iterator for all the wirings in the configuration
     * @return
     */
    public Iterator<Wiring> wiringsIterator() {
	final ArrayList<Wiring> wirings = new ArrayList<Wiring>();

	final Iterator<Edge> edgeIter = this.edges.values().iterator();
	while (edgeIter.hasNext()) {
	    final Wiring w = (Wiring) edgeIter.next();
	    this.addInAlphabeticalOrder(wirings, w);
	}

	return wirings.iterator();
    }

    public void addWiring(final String userID, final String providerID,
	    final String interfaceType) throws CodeGenerationException {
	this.addWiring(userID, providerID, interfaceType, interfaceType,
		interfaceType);
    }

    public void addWiring(final String userID, final String providerID,
	    final String interfaceType, final String typeParameter)
	    throws CodeGenerationException {
	this.addWiring(userID, providerID, interfaceType, typeParameter,
		interfaceType, interfaceType);
    }

    public void addWiring(final String userID, final String providerID,
	    final String interfaceType, final String userAsName,
	    final String providerAsName) throws CodeGenerationException {
	this.addWiring(userID, providerID, interfaceType, null, userAsName,
		providerAsName);
    }

    public String generateEdgeID(final String user, final String userAsName,
	    final String provider, final String providerAsName) {
	return user + "_" + userAsName + "_" + provider + "_" + providerAsName;
    }

    public void addWiring(final String userID, final String providerID,
	    final String interfaceType, final String typeParameter,
	    final String userAsName, final String providerAsName)
	    throws CodeGenerationException {
	final String eid = this.generateEdgeID(userID, userAsName, providerID,
		providerAsName);

	if (!this.nodes.containsKey(userID)) {
	    throw new CodeGenerationException(
		    "Attempt to wire component with userID " + userID
		    + " which is not present in the configuration");
	}

	if (!this.nodes.containsKey(providerID)) {
		    throw new CodeGenerationException(
			    "Attempt to wire component with providerID " + providerID
			    + " which is not present in the configuration");
		}

	if (!super.edges.containsKey(eid)) { //edge not yet in graph
	    final Wiring wiring = new Wiring(eid, userID, providerID);
	    this.edges.put(eid, wiring);
	    wiring.setInterfaceType(interfaceType);
	    wiring.setTypeParameter(typeParameter);
	    wiring.setUserAsName(userAsName);
	    wiring.setProviderAsName(providerAsName);
	}
    }

    public void linkToExternalProvider(final String userID,
	    final String interfaceType, final String userAsName,
	    final String providerAsName) throws CodeGenerationException {
	this.addWiring(userID, OutsideWorldComponent.OUTSIDE_WORLD,
		interfaceType, userAsName, providerAsName);
    }

    public void linkToExternalProvider(final String userID,
	    final String interfaceType, final String typeParameter,
	    final String userAsName, final String providerAsName)
	    throws CodeGenerationException {
	this.addWiring(userID, OutsideWorldComponent.OUTSIDE_WORLD,
		interfaceType, typeParameter, userAsName, providerAsName);
    }

    public void linkToExternalUser(final String providerID,
	    final String interfaceType, final String userAsName,
	    final String providerAsName) throws CodeGenerationException {
	this.addWiring(OutsideWorldComponent.OUTSIDE_WORLD, providerID,
		interfaceType, userAsName, providerAsName);
    }

    /**
     * Exports the graph as a file in the DOT language used by GraphViz,
     * @see http://www.graphviz.org/
     * 
     * @param fname 	the name of the output file
     */
    @Override
    public void exportAsDOTFile(final String fname) {
	try {
	    final PrintWriter out = new PrintWriter(new BufferedWriter(
		    new FileWriter(fname)));

	    final String edgeSymbol = "->";
	    out.println("digraph \"" + this.getName() + "\" {");

	    final Iterator<Node> compIter = this.nodes.values().iterator();
	    while (compIter.hasNext()) {
		final NesCComponent comp = (NesCComponent) compIter.next();
		out.print("\"" + comp.getID() + "\" [fontsize=9 label = \"");

		if (comp instanceof GenericNesCComponent) {
		    out.print(((GenericNesCComponent) comp).getType());
		    out.print("("
			    + ((GenericNesCComponent) comp).getParameter()
			    + ") as  \\n ");
		}

		out.print(comp.getID());

		if (comp.getDescription() != null) {
		    out.print(" \\n " + comp.getDescription());
		}

		out.println("\" ]; ");
	    }

	    //traverse the edges now
	    final Iterator<Wiring> wiringsIter = this.wiringsIterator();
	    while (wiringsIter.hasNext()) {
		final Wiring w = wiringsIter.next();
		out.print("\"" + w.getUser() + "\"" + edgeSymbol + "\""
			+ w.getProvider() + "\" ");

		out.print("[fontsize=9 label = \" ");
		out.print(w.getUserAsName() + " \\n ");
		out.print(w.getInterfaceType());
		if (w.getTypeParameter() != null) {
		    out.print("<" + w.getTypeParameter() + ">");
		}
		out.print("\\n ");
		out.print(w.getProviderAsName() + " \\n\\n ");

		out.println("\"];");
	    }
	    out.println("}");
	    out.close();
	} catch (final IOException e) {
	    System.err.println("Export failed: " + e.toString());
	}
    }

    public void mergeGraphs(final NesCConfiguration config) {

	final Iterator<Node> compIter = config.nodes.values().iterator();
	while (compIter.hasNext()) {
	    this.addComponent((NesCComponent) compIter.next());
	}

	super.mergeGraphs(config);
    }

    public void exportConfigurationAsNesCFile(final String fname,
	    final String configComponentName,
	    final boolean displayOperatorComments) {
	try {
		
	    final PrintWriter out = new PrintWriter(new BufferedWriter(
		    new FileWriter(fname)));

	    final StringBuffer externalUsesBuffer = new StringBuffer();
	    final StringBuffer externalProvidesBuffer = new StringBuffer();
	    final StringBuffer componentsBuffer = new StringBuffer();
	    final StringBuffer componentCommentsBuffer = new StringBuffer();
	    final StringBuffer wiringsBuffer = new StringBuffer();
	    String typeParamTempStr = null;

	    final Iterator<NesCComponent> compIter = this.componentIterator();
	    while (compIter.hasNext()) {

		final NesCComponent comp = compIter.next();
		final String compName = comp.getID();
		if (!compName.equals(OutsideWorldComponent.OUTSIDE_WORLD)) {
		    componentsBuffer.append(comp.getDeclaration());

		    //					if (displayOperatorComments) {
		    //						componentCommentsBuffer.append(comp.getDescription());

		    //					    "\t// "+compName+
		    //							" = "+((Operator)plan.getNode(opID)).getText(false)+"\n");
		    //					}
		}
	    }

	    //quick fix to say Leds have not yet been wired.
	    boolean unwiredLed = true;
	    
	    final Iterator<Wiring> wiringsIter = this.wiringsIterator();
	    while (wiringsIter.hasNext()) {
		final Wiring w = wiringsIter.next();

		//the key is used for sorting the wirings to improve legibility
		final String key = w.getID();

		if (w.getUser().equals(OutsideWorldComponent.OUTSIDE_WORLD)) {
		    if (externalProvidesBuffer.length() == 0) {
			externalProvidesBuffer.append("\tprovides\n");
			externalProvidesBuffer.append("\t{\n");
		    }
		    externalProvidesBuffer.append("\t\t interface "
			    + w.getInterfaceType() + " as " + w.getUserAsName()
			    + ";\n");

		    wiringsBuffer.append("\t" + w.getUserAsName() + " = "
			    + w.getProvider() + "." + w.getProviderAsName()
			    + ";\n");

		} else if (w.getProvider().equals(
			OutsideWorldComponent.OUTSIDE_WORLD)) {

			if (!w.getProviderAsName().equals("Leds") || unwiredLed) { 
			if (externalUsesBuffer.length() == 0) {
			externalUsesBuffer.append("\tuses\n");
			externalUsesBuffer.append("\t{\n");
		    }

		    if ((w.getTypeParameter() != null)) {

			typeParamTempStr = "<" + w.getTypeParameter() + "> ";
			externalUsesBuffer.append("\t\t interface "
				+ w.getInterfaceType() + typeParamTempStr
				+ " as " + w.getProviderAsName() + ";\n");
		    } else {
			externalUsesBuffer.append("\t\t interface "
				+ w.getInterfaceType() + " as "
				+ w.getProviderAsName() + ";\n");
		    }
			}//if (!w.getProviderAsName().equals("Leds") || unwiredLed)
			
			if (w.getProviderAsName().equals("Leds")) {
				unwiredLed = false;
			}

			wiringsBuffer.append("\t" + w.getProviderAsName() + " = "
			    + w.getUser() + "." + w.getUserAsName() + ";\n");
			


		} else {
		    wiringsBuffer.append("\t" + w.getUser() + "."
			    + w.getUserAsName() + " -> " + w.getProvider()
			    + "." + w.getProviderAsName() + ";\n");
		}
	    }

	    //start dumping to file

    	out.println("#include \"QueryPlan.h\"");
    	out.println("#include <hardware.h>\n");	    	
	    
	    out.println("\n\nconfiguration " + configComponentName + " {");

	    if (externalProvidesBuffer.length() > 0) {
		externalProvidesBuffer.append("\t}\n");
		out.println(externalProvidesBuffer);
	    }
	    if (externalUsesBuffer.length() > 0) {
		externalUsesBuffer.append("\t}\n");
		out.println(externalUsesBuffer);
	    }

	    out.println("}");
	    out.println("implementation");
	    out.println("{");

	    out.println(componentsBuffer);
	    if (displayOperatorComments) {
		out.println(componentCommentsBuffer + "\n");
	    }

	    out.println(wiringsBuffer);

	    out.println("}");
	    out.close();
	} catch (final IOException e) {
	    System.err.println("Export failed: " + e.toString());
	}
    }

    public String generateModuleHeader(final String moduleName) {

	final StringBuffer resultBuff = new StringBuffer();
	final StringBuffer incomingBuff = new StringBuffer();
	final StringBuffer outgoingBuff = new StringBuffer();

	resultBuff.append("#include \"QueryPlan.h\"\n\n");
	resultBuff.append("module " + moduleName + "\n");
	resultBuff.append("{\n");

	//used to avoid duplicates when we have fan-ins or fan-outs
	final HashSet<String> providerAsNames = new HashSet<String>();
	final HashSet<String> userAsNames = new HashSet<String>();

	final Iterator<Wiring> wiringsIter = this.wiringsIterator();
	while (wiringsIter.hasNext()) {
	    final Wiring w = wiringsIter.next();

	    String typeParamStr = "";
	    if (w.getTypeParameter() != null) {
		typeParamStr = "<" + w.getTypeParameter() + ">";
	    }

	    if (w.getUser().equals(moduleName)) {

		if (!userAsNames.contains(w.getUserAsName())) { //TODO: check
			if (w.getInterfaceType().endsWith("();")) {
				//Not an interface; a command or an event
				outgoingBuff.append("\t\t" + w.getInterfaceType() + "\n");
			} else {
			    outgoingBuff
			    .append("		interface " + w.getInterfaceType()
				    + typeParamStr + " as " + w.getUserAsName()
				    + ";\n");
			}
		    userAsNames.add(w.getUserAsName());
		}
	    } else if (w.getProvider().equals(moduleName)) {

			if (!providerAsNames.contains(w.getProviderAsName())) {
				if (w.getInterfaceType().endsWith("();")) {
					//Not an interface; a command or an event
					outgoingBuff.append("\t\t" + w.getInterfaceType() + "\n");				
				} else {
					incomingBuff.append("		interface " + w.getInterfaceType()
						    + typeParamStr + " as " + w.getProviderAsName()
						    + ";\n");
				}
				
			    providerAsNames.add(w.getProviderAsName());
			}
	    }
	}

	if (!userAsNames.isEmpty()) {
	    resultBuff.append("\tuses\n");
	    resultBuff.append("\t{\n");
	    resultBuff.append(outgoingBuff);
	    resultBuff.append("\t}\n");
	}

	if (!providerAsNames.isEmpty()) {
	    resultBuff.append("\tprovides\n");
	    resultBuff.append("\t{\n");
	    resultBuff.append(incomingBuff);
	    resultBuff.append("\t}\n");
	}

	resultBuff.append("}\n");
	resultBuff.append("implementation\n");
	resultBuff.append("{\n\n");

	//	resultBuff.append("\tchar timeBuf[128];\n");	

	return resultBuff.toString();
    }

    public Iterator<String> getComponentIDIter() {
	return this.nodes.keySet().iterator();
    }

    /**
     * Instantiates every component in the configuration.  If any components are themselves
     * configurations, these are recursively instantiated as well.
     * @throws OptimizationException 
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     * @throws URISyntaxException 
     *
     */
    public void instantiateComponents(final String outputDir) throws IOException,
	    CodeGenerationException, SchemaMetadataException, TypeMappingException, OptimizationException, URISyntaxException {
	
	Utils.checkDirectory(outputDir, true);

	this.exportConfigurationAsNesCFile(outputDir + this.name + ".nc", this
		.getName(), false);
	
	final Iterator<NesCComponent> compIter = this.componentIterator();
	while (compIter.hasNext()) {
	    final NesCComponent comp = compIter.next();

	    //a bit of a hack, so that a query plan module knows about the whole configuration
	    if (comp instanceof QueryPlanModuleComponent) {
		((QueryPlanModuleComponent) comp).setTossimConfig(this);
	    }

	    comp.writeNesCFile(outputDir);
	}
    }

}
