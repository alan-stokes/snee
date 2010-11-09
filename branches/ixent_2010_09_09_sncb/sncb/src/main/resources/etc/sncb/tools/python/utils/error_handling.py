import sys

E_SUCCESS = 0
E_FAIL = 2

def handleReturnCode(retcode, msg=None):
    if retcode == E_SUCCESS:
        print 'E_SUCCESS'        
    else:
        print 'E_FAIL: ' + str(msg)
        sys.exit(E_FAIL)
