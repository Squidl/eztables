import matplotlib.pyplot as plt
import matplotlib.patches
patches=matplotlib.patches

from mpl_toolkits.mplot3d import Axes3D

namespacecolors=[
    [1,0,0],
    [0,1,0],
    [0,0,1],
    [1,1,0],
    [1,0,1],
    [1,.5,0],
    [1,.8,.85],
    [0,1,1],
    [0,0,0],
    [.5,.5,.5],
    [float(150)/255,float(75)/255,0]
]

def get_rowwise(table,row,glob,maps={}):
    data = glob.copy()
    keys = row.keys()
    cols = [row[k] for k in keys]
    tags = [table.findcol(c) for c in cols]
    rowdata = []
    for tag in tags:
        rowdata.append([])
    for irow in table.data:
        for j in range(len(tags)):
            curr=irow[tags[j]]
            if keys[j] in maps:
                curr=maps[keys[j]][curr]
            rowdata[j].append(curr)
    for keyword in row:
        index=cols.index(row[keyword])
        data[keyword]=rowdata[index]
    return data

def scatter3d(table,row,glob,cmap=None):
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')
    data=get_rowwise(table,row,glob)
    xlab=row["xs"]
    ylab=row["ys"]
    zlab=row["zs"]
    if cmap is not None:
        data["c"]=[cmap[x] for x in data["c"]]
    ax.scatter(**data)
    ax.set_xlabel(xlab)
    ax.set_ylabel(ylab)
    ax.set_zlabel(zlab)
    return plt

class valmap:
    def __init__(self):
        self.data={}
        self.counter=0
    def __getitem__(self,ind):
        try:
            find=float(ind)
            return find
        except:
            if not ind in self.data:
                self.data[ind]=self.counter
                self.counter+=1
            return self.data[ind]

def scatter(table,row,glob,cmap=None,classes=None):
    cind=None
    labproxies=[]
    maps={}
    for val in ["xs","ys","zs"]:
        if val in row:
            typer = table.types[table.findcol(row[val])]
            if not (typer.label=="float" or typer.label=="int"):
                maps[val]=valmap()
    if classes is None and "c" in row:
        classes=table.get_distinct(row["c"])
        cind=table.findcol(row["c"])
        del row["c"]
    if cmap is None and classes!=None:
        cmap=[namespacecolors[i] for i in range(len(classes))]
    if classes is None:
        classes=[None]
    if "zs" in row or "zs" in glob:
        fig = plt.figure()
        ax = fig.add_subplot(111, projection='3d')
    else:
        fig=plt.figure()
        ax=fig.add_subplot(111,xlabel=row["xs"],ylabel=row["ys"])
    for i in range(len(classes)):
        thisclass=classes[i]
        cond=lambda x:True
        if thisclass!=None:
            cond=(lambda x: x[cind]==thisclass)
        query=table.selection(cond)
        data=get_rowwise(query,row,glob,maps)
        if cmap is not None:
            data["c"]=cmap[i]
        data["label"]=thisclass
        if "marker" not in data:
            data["marker"]="o"
        if "zs" not in data:
            xvals=data["xs"]
            del data["xs"]
            yvals=data["ys"]
            del data["ys"]
            plt.scatter(xvals,yvals,**data)
        else:
            ax.scatter(**data)
        lab=plt.Rectangle( (0,0), 1, 1, color=data["c"])
        labproxies.append( (lab,thisclass) )
    if "zs" in data:
        ax.set_xlabel(row["xs"])
        ax.set_ylabel(row["ys"])
        ax.set_zlabel(row["zs"])
        ax.legend([p[0] for p in labproxies],[p[1] for p in labproxies] )
    else:
        plt.legend([p[0] for p in labproxies],[p[1] for p in labproxies] )
    return plt

def lineplot(table,row,glob):
    fig=plt.figure()
    ax=fig.add_subplot(111,xlabel=row["xs"],ylabel=row["ys"])
    data=get_rowwise(table,row,glob)
    xs=data["xs"]
    del data["xs"]
    ys=data["ys"]
    del data["ys"]
    plt.plot(xs,ys,**data)
    return plt
