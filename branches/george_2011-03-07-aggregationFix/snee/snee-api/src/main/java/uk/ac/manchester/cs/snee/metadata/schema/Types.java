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
package uk.ac.manchester.cs.snee.metadata.schema;

/*
 * @(#)DomEcho02.java	1.9 98/11/10
 *
 * Copyright (c) 1998 Sun Microsystems, Inc. All Rights Reserved.
 *
 * Sun grants you ("Licensee") a non-exclusive, royalty free, license to use,
 * modify and redistribute this software in source and binary code form,
 * provided that i) this copyright notice and license appear on all copies of
 * the software; and ii) Licensee does not utilize the software in a manner
 * which is disparaging to Sun.
 *
 * This software is provided "AS IS," without a warranty of any kind. ALL
 * EXPRESS OR IMPLIED CONDITIONS, REPRESENTATIONS AND WARRANTIES, INCLUDING ANY
 * IMPLIED WARRANTY OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE OR
 * NON-INFRINGEMENT, ARE HEREBY EXCLUDED. SUN AND ITS LICENSORS SHALL NOT BE
 * LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING
 * OR DISTRIBUTING THE SOFTWARE OR ITS DERIVATIVES. IN NO EVENT WILL SUN OR ITS
 * LICENSORS BE LIABLE FOR ANY LOST REVENUE, PROFIT OR DATA, OR FOR DIRECT,
 * INDIRECT, SPECIAL, CONSEQUENTIAL, INCIDENTAL OR PUNITIVE DAMAGES, HOWEVER
 * CAUSED AND REGARDLESS OF THE THEORY OF LIABILITY, ARISING OUT OF THE USE OF
 * OR INABILITY TO USE SOFTWARE, EVEN IF SUN HAS BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGES.
 *
 * This software is not designed or intended for use in on-line control of
 * aircraft, air traffic, aircraft navigation or aircraft communications; or in
 * the design, construction, operation or maintenance of any nuclear
 * facility. Licensee represents and warrants that it will not use or
 * redistribute the Software for such purposes.
 */

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class Types {
	
	private Logger logger = Logger.getLogger(Types.class.getName());

	private Map<String, AttributeType> _types = 
		new Hashtable<String, AttributeType>();

	//XXX-AG: Changed from private to public.
	public Types(String typesFilename) throws TypeMappingException {
		if (logger.isDebugEnabled())
			logger.debug("ENTER Types() with file " + typesFilename);
		
		Document doc = parseFile(typesFilename);
		Element root = (Element) doc.getFirstChild();
		NodeList xmlTypes = root.getElementsByTagName("type");
		for (int i = 0; i < xmlTypes.getLength(); i++) {
			AttributeType type = 
				new AttributeType((Element) xmlTypes.item(i));
			_types.put(type.getName(), type);
			if (logger.isTraceEnabled())
				logger.trace("Added type " + type.getName());
		}

		if (logger.isInfoEnabled())
			logger.info("Types successfully read in from " + typesFilename);
		
		if (logger.isDebugEnabled())
			logger.debug("RETURN Types()");
	}

	private Document parseFile(String typesFilename) 
	throws TypeMappingException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER parseFile() with file " + typesFilename);
		try {
			DocumentBuilderFactory factory = 
				DocumentBuilderFactory.newInstance();
			DocumentBuilder builder;
			builder = factory.newDocumentBuilder();
			Document doc = builder.parse(typesFilename);
			if (logger.isTraceEnabled())
				logger.trace("RETURN");
			return doc;
		} catch (ParserConfigurationException e) {
			String msg = "Problem parsing types file.";
			logger.warn(msg);
			throw new TypeMappingException(msg, e);
		} catch (SAXException e) {
			String msg = "Problem parsing types file.";
			logger.warn(msg);
			throw new TypeMappingException(msg, e);
		} catch (IOException e) {
			String msg = "Problem locating types file.";
			logger.warn(msg);
			throw new TypeMappingException(msg, e);
		}
	}

	public AttributeType getType(String name) 
	throws TypeMappingException, SchemaMetadataException {
		AttributeType type;
		if (_types.containsKey(name)) {
			type = _types.get(name);			
		} else {
			String message = "Type \"" + name + "\" not found";
			logger.warn(message);
			throw new SchemaMetadataException(message);
		}
		return type;
	}

	AttributeType tupleType(Map<String, AttributeType> attributes) {
		String name = "tuple";
		int size = 0;
		int math = 0;
		int factor = 0;
		int length = 1;
		Iterator<AttributeType> it = attributes.values().iterator();
		while (it.hasNext()) {
			size = size + it.next().getSize();
		}
		return new AttributeType(name, size, math, factor, length, 
				"tuple", false);
	}

}