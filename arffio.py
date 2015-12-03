import eztables
from iotables import tableinput, tableoutput, extentiondict

class arfftableinput(tableinput):
    def __init__(self,
                 stream,
                 callback=None,
                 **kwargs):
        self.f=stream
        self.callback=callback
        names=[]
        types=[]
        while True:
            line=self.f.readline()
            line=line.strip()
            if len(line)<=0 or line[0]=='%':
                continue
            if line.upper()=="@DATA":
                break
            splits=line.split()
            if splits[0].upper()=="@ATTRIBUTE":
                names.append(splits[1])
                if splits[2].lower()=="numeric":
                    types.append(float)
                else:
                    types.append(None)
        self.tabledef=eztables.tabledef(names,types,[])
        self.spent=False
    def getdef(self):
        return self.tabledef
    def __iter__(self):
        if self.spent==False:
            self.spent=True
            return self
        else:
            print("need to reset stream")
            return None
    def next(self):
        """Load rows from the filestream. This is a seperate
        function so that it can be called asynchronously"""
        line = self.f.readline()
        if len(line) == 0:
            raise StopIteration
        newrow=line.split(",")
        return newrow
    def close(self):
        self.f.close()
class arfftableoutput(tableoutput):
    def __init__(self,
                 stream,
                 name=None):
        self.stream=stream
        self.name=name
        if self.name is None:
            self.name="data"
    def head(self,table):
        self.stream.write("@RELATION %s\n\n"%self.name)
        for i in range(len(table.columns)):
            col=table.columns[i]
            ctype=None
            if table.types[i].label=="float":
                ctype="numeric"
            else:
                ctype="{%s}"%(",".join([str(x) for x in table.get_distinct(col)]))
            self.stream.write("@ATTRIBUTE %s %s\n"%(col,ctype))
        self.stream.write("\n@DATA\n")
    def write(self,row):
        self.stream.write(",".join([str(i) for i in row])+"\n")
    def final(self,table):
        pass
    def close(self):
        self.stream.close()

extentiondict["arff"]=(arfftableinput,arfftableoutput)
