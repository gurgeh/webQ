import sys
import shlex
import threading
import re

CONTAINER_CHARS = '{[('

def make_type(x):
    if not x: return x

    if x[0] in CONTAINER_CHARS:
        try: return eval(x)
        except: pass
    
    try: return int(x)
    except ValueError: pass

    try: return float(x)
    except ValueError: pass
    
    return x
    

class ComLine:
    def __init__(self, spawn=True):
        self.alive = True

        if spawn:
            self._cline = threading.Thread(target=self._process_commands)
            self._cline.start()
        else:
            self._process_commands()
        
    def _process_commands(self):
        while self.alive:
            line = sys.stdin.readline()
            if not line:
                self.alive = False
            else:
                args = shlex.split(line)
                if not args:
                    continue
                command = args[0]
                kwargs = [x.split('=', 1) for x in args[1:] if '=' in x]
                kwargs = dict([(key, make_type(val)) for key,val in kwargs])
                args = [make_type(x) for x in args[1:] if '=' not in x]
                try:
                    x = getattr(self, command)(*args, **kwargs)
                    if x is not None:
                        print '>>', x
                except Exception, e:
                    print 'Exception', args, str(e)

    def quit(self):
        self.alive = False

    def ping(self, word):
        print 'pong', word
                
            
if __name__ == '__main__':
    c = ComLine(False)
    
