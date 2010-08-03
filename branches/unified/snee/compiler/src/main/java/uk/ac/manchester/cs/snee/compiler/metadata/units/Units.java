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
package uk.ac.manchester.cs.snee.compiler.metadata.units;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.TreeMap;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;
import uk.ac.manchester.cs.snee.common.Utils;

// This class is based on the Bill Pugh singleton design pattern, http://en.wikipedia.org/wiki/Singleton_pattern
public class Units {

	TreeMap<String, Long> timeScalingFactors;

	TreeMap<String, Long> memoryScalingFactors;

	TreeMap<String, Long> energyScalingFactors;

	private Units() {

		this.timeScalingFactors = new TreeMap<String, Long>();
		this.energyScalingFactors = new TreeMap<String, Long>();
		this.memoryScalingFactors = new TreeMap<String, Long>();

		try {
			this.parseXMLFile();
		} catch (XPathExpressionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SNEEConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void parseXMLFile() 
	throws ParserConfigurationException, SAXException, IOException, 
	XPathExpressionException, SNEEConfigurationException {
		Utils.validateXMLFile(
				SNEEProperties.getSetting(SNEEPropertyNames.INPUTS_UNITS_FILE),
				"input/units.xsd");

		// parse file
		String timeBaseUnit = Utils.doXPathStrQuery(
				SNEEProperties.getSetting(SNEEPropertyNames.INPUTS_UNITS_FILE),
				"/snee:units/snee:time/snee:base-unit");
		this.setScalingFactor(timeBaseUnit.toUpperCase(), new Long(1),
				this.timeScalingFactors);

		String energyBaseUnit = Utils.doXPathStrQuery(
				SNEEProperties.getSetting(SNEEPropertyNames.INPUTS_UNITS_FILE),
				"/snee:units/snee:energy/snee:base-unit");
		this.setScalingFactor(energyBaseUnit.toUpperCase(), new Long(1),
				this.energyScalingFactors);

		String memoryBaseUnit = Utils.doXPathStrQuery(
				SNEEProperties.getSetting(SNEEPropertyNames.INPUTS_UNITS_FILE),
				"/snee:units/snee:memory/snee:base-unit");
		this.setScalingFactor(memoryBaseUnit.toUpperCase(), new Long(1),
				this.memoryScalingFactors);

		this.setScalingFactors("/snee:units/snee:time/snee:unit",
				this.timeScalingFactors);
		this.setScalingFactors("/snee:units/snee:energy/snee:unit",
				this.energyScalingFactors);
		this.setScalingFactors("/snee:units/snee:memory/snee:unit",
				this.memoryScalingFactors);
	}

	private void setScalingFactors(String context,
			TreeMap<String, Long> unitScalingFactors)
	throws XPathExpressionException, FileNotFoundException,
	SNEEConfigurationException {
		NodeList nodeList = Utils.doXPathQuery(
				SNEEProperties.getSetting(SNEEPropertyNames.INPUTS_UNITS_FILE), context);
		for (int i = 0; i < nodeList.getLength(); i++) {
			Node node = nodeList.item(i);
			String unitName = Utils.doXPathStrQuery(node, "snee:name");
			Long scalingFactor = new Long(Utils.doXPathStrQuery(node,
			"snee:scaling-factor"));
			this.setScalingFactor(unitName, scalingFactor, unitScalingFactors);
		}
	}

	private void setScalingFactor(String unitName,
			Long scalingFactor,
			TreeMap<String, Long> unitScalingFactors) {
		//Add two versions, one plural, one singular, because users will get it wrong!
		unitScalingFactors.put(unitName, scalingFactor);
		if (unitName.toUpperCase().endsWith("S")) {
			unitScalingFactors.put(unitName.toUpperCase().substring(0,
					unitName.length() - 1), scalingFactor);
		} else {
			unitScalingFactors.put(unitName.toUpperCase() + "S", scalingFactor);
		}
	}

	private static class UnitsHolder {
		private static Units instance = new Units();
	}

	public static Units getInstance() {
		return UnitsHolder.instance;
	}

	public long getTimeScalingFactor(String unitName)
	throws UnrecognizedUnitException {
		if (this.timeScalingFactors.containsKey(unitName.toUpperCase())) {
			return this.timeScalingFactors.get(unitName.toUpperCase())
			.longValue();
		} else {
			throw new UnrecognizedUnitException(unitName
					+ " not a recognized time unit");
		}
	}

	public long getEnergyScalingFactor(String unitName)
	throws UnrecognizedUnitException {
		if (this.energyScalingFactors.containsKey(unitName.toUpperCase())) {
			return this.energyScalingFactors.get(unitName.toUpperCase())
			.longValue();
		} else {
			throw new UnrecognizedUnitException(unitName
					+ " not a recognized energy unit");
		}
	}

	public long getMemoryScalingFactor(String unitName)
	throws UnrecognizedUnitException {
		if (this.memoryScalingFactors.containsKey(unitName.toUpperCase())) {
			return this.memoryScalingFactors.get(unitName.toUpperCase())
			.longValue();
		} else {
			throw new UnrecognizedUnitException(unitName
					+ " not a recognized memory unit");
		}
	}

	public long getScalingFactor(String unitName)
	throws UnrecognizedUnitException {

		// some values don't have units, e.g., buffering factor
		if (unitName == null || unitName.equals("null")) {
			return 1;
		}

		if (this.timeScalingFactors.containsKey(unitName.toUpperCase())) {
			return this.timeScalingFactors.get(unitName.toUpperCase())
			.longValue();
		} else if (this.energyScalingFactors
				.containsKey(unitName.toUpperCase())) {
			return this.energyScalingFactors.get(unitName.toUpperCase())
			.longValue();
		} else if (this.memoryScalingFactors
				.containsKey(unitName.toUpperCase())) {
			return this.memoryScalingFactors.get(unitName.toUpperCase())
			.longValue();
		} else {
			throw new UnrecognizedUnitException(unitName
					+ " not a recognized memory unit");
		}
	}
}
