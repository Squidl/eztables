import os

def avail(filepath
          ,last=False
          ,start=1
          ,template="%s_%s"
          ,timestamp=True
          ,step=1):
    counter=start
    lastfilepath=filepath
    lastupdate=None
    if os.path.isfile(filepath):
        stats=os.stat(filepath)
        lastupdate=stats.st_mtime
    else:
        return filepath if not last else None
    pathparts=filepath.split(".")
    pathparts=(".".join(pathparts[:-1]),pathparts[-1])
    while True:
        currfilepath=(template % (pathparts[0],str(counter)) )+"."+pathparts[1]
        if os.path.isfile(currfilepath):
            stats=os.stat(currfilepath)
            if timestamp and lastupdate>stats.st_mtime:
                break
            lastupdate=stats.st_mtime
            lastfilepath=currfilepath
        else:
            break
        counter=counter+step
    return currfilepath if not last else lastfilepath
