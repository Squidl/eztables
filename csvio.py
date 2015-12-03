import eztables
from iotables import tableinput, tableoutput, extentiondict

class eztableinput(tableinput):
    def __init__(self,
                 stream,
                 names=None,
                 types=None,
                 alltype=None,
                 callback=None,
                 **kwargs):
        self.f=stream
        self.data=None
        self.callback=callback
        if names is None:
            names=[]
            typed=None
            if types is None:
                types=[]
                typed=False
            cols=self.f.readline().rstrip("\n").split(",")
            for i in cols:
                name=i.strip()
                if not typed and "^" in name:
                    namesparts=name.split("^")
                    names.append(namesparts[0])
                    types.append(types_dict[namesparts[1]])
                elif not typed:
                    names.append(name)
                    types.append(None)
                else:
                    names.append(name)
            if alltype is not None:
                types=[alltype]*len(names)
        else:
            if types is None:
                types=[None]*len(names)
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

class eztableoutput(tableoutput):
    def __init__(self,
                 stream,
                 withtypes=True):
        self.stream=stream
        self.withtypes=withtypes
    def head(self,table):
        colsprint=[]
        for i in range(len(table.columns)):
            if (not self.withtypes) or table.types[i] is None or table.types[i].pytype is None:
                colsprint.append(table.columns[i])
            else:
                colsprint.append(table.columns[i]+"^"+table.types[i].label)
        self.stream.write(",".join(colsprint)+"\n")
    def write(self,row):
        self.stream.write(",".join([str(i) for i in row])+"\n")
    def final(self,table):
        pass
    def close(self):
        self.stream.close()
        
extentiondict["csv"]=(eztableinput,eztableoutput)
