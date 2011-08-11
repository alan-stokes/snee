import sys

E_SUCCESS = 0
E_FAIL = 1

def handleReturnCode(retcode, msg=None):
    if retcode == E_SUCCESS:
        print 'E_SUCCESS: ' + 'Operation completed successfully'
#    elif retcode == E_NO_ROUTE:
#        print 'E_NO_ROUTE: ' + str(msg)
#        sys.exit(E_NO_ROUTE)
#    elif retcode == E_NO_REPLY:
#        print 'E_NO_REPLY: ' + str(msg)
#        sys.exit(E_NO_REPLY)
    else:
        print 'E_FAIL: ' + str(retcode) + ' ' + str(msg)
        sys.exit(E_FAIL)
