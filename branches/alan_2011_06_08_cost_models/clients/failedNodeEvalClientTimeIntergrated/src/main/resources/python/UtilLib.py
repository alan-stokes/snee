import os, time, string, sys

 
logger = None
  		
def registerLogger(l):
 	global logger
 	logger = l

def report(message):
 	if (logger != None):
 		logger.info (message)
 	print message

#Ouput info message to screen and logger if applicable
def report(message):
 	if (logger != None):
 		logger.info (message)
 	print message

#Ouput warning message to screen and logger if applicable
def reportWarning(message):
 	if (logger != None):
 		logger.warning(message)
 	print message


#Ouput error message to screen and logger if applicable
def reportError(message):
 	if (logger != None):
 		logger.error(message)
 	print message

 
def runCommand(commandStr):
	report ("Executing " + commandStr)
 	exitVal = os.system(commandStr)
	return exitVal
	
def winpath(pathStr):
	winpath = pathStr.replace("/cygdrive/c/", "c:/")
	winpath = winpath.replace("/cygdrive/d/", "d:/")
	winpath = winpath.replace("/cygdrive/e/", "e:/")
	winpath = winpath.replace("/cygdrive/f/", "f:/")
	return winpath
	
def getTimeStamp():
	return time.strftime("%d%b%Y-%H-%M-%S")
	
#Converts a string to boolean based on the value of the first character.
def convertBool(text):
	if text == '':
		return False
	if text[0] in ('T','t'):
		return True
	if text[0] in ('F','f'):
		return False
	print 'Cannot convert "'+text+'" to boolean value'
	sys.exit(1)

def getFileContents(filePath):
	f = open(filePath, "r")
	str = f.readlines()
	return string.join(str,"")
	
def removeDuplicates(seq): 
    # order preserving
    seen = {}
    result = []
    for item in seq:
        if item in seen: 
        	continue
        seen[item] = 1
        result.append(item)
    return result

#Converts seconds to days
def secondsToDays(secs):
	days = float(secs)/60.0/60.0/24.0
	return days
	
def monthsToSeconds(months):
	secs = months*30.5*24*60*60
	return secs
