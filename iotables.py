import streamlist
import eztables

import os
 
extentiondict={}


class tableinput:
    def getdef(self):
        pass
    def __iter__(self):
        pass
    def close(self):
        pass

class tableoutput:
    def head(self):
        pass
    def write(self):
        pass
    def final(self):
        pass
    def close(self):
        pass

def getextinp(filepath):
    ext=filepath.split(".")[-1]
    return extentiondict[ext][0]
def getextout(filepath):
    ext=filepath.split(".")[-1]
    return extentiondict[ext][1]

import csvio
import arffio

class tabtableoutput(tableoutput):
    def __init__(self,
                 stream,
                 withtypes=True):
        self.stream=stream
        self.withtypes=withtypes
    def head(self,table):
        colsprint=[]
        for i in range(len(table.columns)):
            if not self.withtypes:
                colsprint.append(table.columns[i])
            else:
                colsprint.append(table.columns[i]+"^"+table.types[i].label)
        self.stream.write("\t".join(colsprint)+"\n")
    def write(self,row):
        self.stream.write("\t".join([str(i) for i in row])+"\n")
    def final(self,table):
        pass
    def close(self):
        self.stream.close()

class printtableoutput(tableoutput):
    def __init__(self,
                 stream,
                 withtypes=True,
                 width=None):
        self.stream=stream
        self.withtypes=withtypes
        if width is None:
            if os.isatty(1):
                try:
                    import fcntl, termios, struct
                    width=struct.unpack('hh', fcntl.ioctl(1, termios.TIOCGWINSZ, '1234'))[1]
                    print(width)
                except:
                    try:
                        width=os.environ['LINES']
                    except:
                        pass
            else:
                width=999
        self.width=width
    def head(self,table):
        coltypes=table.types
        colnames=table.columns
        coldiv=(self.width-1)//len(colnames)
        colrem=self.width-(coldiv*len(colnames))-1
        colsizes=[coldiv-2 if i<colrem else coldiv-3 for i in range(len(colnames))]
        self.colsizes=colsizes
        colsprint=[]
        for i in range(len(colnames)):
            data=colnames[i][:colsizes[i]]
            if len(data)<colsizes[i]:
                data=data+(" "*(colsizes[i]-len(data)))
            colsprint.append(data)
        self.stream.write("|"+("|".join(colsprint))+"|\n")
        self.stream.write("-"*self.width)
    def write(self,row):
        colsprint=[]
        for i in range(len(row)):
            data=str(row[i])[:self.colsizes[i]]
            if len(data)<self.colsizes[i]:
                data=data+(" "*(self.colsizes[i]-len(data)))
            colsprint.append(data)
        self.stream.write("| "+(" | ".join(colsprint))+" |\n")
        self.stream.write("-"*self.width)
    def final(self,table):
        pass
    def close(self):
        self.stream.close()


class latextableoutput(tableoutput):
    def __init__(self,stream):
        self.stream=stream
    def head(self,table):
        self.stream.write("\\begin{tabular}{|"+"|".join(["c"]*len(table.columns))+"|} \\hline \n")
        self.stream.write("&".join([str(i) for i in table.columns])+"\\\\ \\hline \\hline \n")
    def write(self,row):
        self.stream.write("&".join([str(i) for i in row])+"\\\\ \\hline \n")
    def final(self,table):
        self.stream.write("\\end{tabular}")
    def close(self):
        self.stream.close()
