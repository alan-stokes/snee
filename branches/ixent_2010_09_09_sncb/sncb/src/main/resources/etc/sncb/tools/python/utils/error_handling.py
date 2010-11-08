import sys

E_SUCCESS = 0
E_FAIL = 1

def handleReturnCode(retcode, msg=None):
    if retcode == E_SUCCESS:
    elif retcode == E_FAIL:
        print 'E_FAIL: ' + str(msg)
        sys.exit(E_FAIL)
        
