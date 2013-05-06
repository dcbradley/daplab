
import os

def which(command):
    dirname = os.path.dirname(command)
    if dirname:
        path = (dirname)
        command = os.path.basename(command)
    else:
        path = os.environ["PATH"].split(os.pathsep)

    for p in path:
        f = os.path.join(p,command)
        if os.path.isfile(f) and os.access(f,os.X_OK):
            return f

    raise Exception("No " + command + " in " + str(path))
