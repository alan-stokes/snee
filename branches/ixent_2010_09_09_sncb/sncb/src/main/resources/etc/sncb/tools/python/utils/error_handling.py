import sys

E_SUCCESS = 0
E_INSTALLATION_ERROR = 1
E_MAKE_ERROR = 2

def handleReturnCode(retcode, msg=None):
    if retcode == E_SUCCESS:
        print 'E_SUCCESS'        
    elif retcode == E_INSTALLATION_ERROR:
        print 'E_INSTALLATION_ERROR: ' + str(msg)
        sys.exit(E_INSTALLATION_ERROR)        
    elif retcode == E_MAKE_ERROR:
        print 'E_MAKE_ERROR: ' + str(msg)
        sys.exit(E_MAKE_ERROR)
