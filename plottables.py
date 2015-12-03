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
    [0,0,0],
    [.5,.5,.5],
    [150/255,75/255,0],
    [1,.5,0],
    [1,.8,.85],
    [0,1,1]
]

def get_rowwise(table,row,glob):
    data = glob.copy()
    cols = [row[k] for k in row.keys()]
    tags = [table.findcol(c) for c in cols]
    rowdata = []
    for tag in tags:
        rowdata.append([])
    for irow in table.data:
        for j in range(len(tags)):
            rowdata[j].append(irow[tags[j]])
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

def scatter(table,row,glob,cmap=None,classes=None):
    xs=None
    if "xs" in row:
        xs=row["xs"]
        del row["xs"]
    elif "xs" not in glob:
        print("neex x value")
    ys=None
    if "ys" in row:
        ys=row["ys"]
        del row["ys"]
    elif "ys" not in glob:
        print("need y value")
    zs=None
    if "zs" in row:
        zs=row["zs"]
        del row["zs"]
    cind=None
    if classes is None and "c" in row:
        classes=table.get_distinct(row["c"])
        cind=table.findcol(row["c"])
        del row["c"]
    if cmap is None and classes!=None:
        cmap=[namespacecolors[i] for i in range(len(classes))]
    data=get_rowwise(table,row,glob)
    if not "marker" in data:
        data["marker"]="o"
    data["linestyle"]="None"
    if classes is None:
        classes=[None]
    if zs is not None or "zs" in glob:
        fig = plt.figure()
        ax = fig.add_subplot(111, projection='3d')
    else:
        fig=plt.figure()
        ax=fig.add_subplot(111,xlabel=xs,ylabel=ys)
    for i in range(len(classes)):
        thisclass=classes[i]
        tofetch={}
        if xs!=None:
            tofetch["xs"]=xs
        if ys!=None:
            tofetch["ys"]=ys
        if zs!=None:
            tofetch["zs"]=zs
        if len(tofetch.keys())>0:
            ks=tofetch.keys()
            cols=[tofetch[k] for k in ks]
            cond=lambda x:True
            if thisclass!=None:
                cond=(lambda x: x[cind]==thisclass)
            query=table.selection(cond)
            query.project(cols)
            additive={}
            for k in ks:
                additive[k]=[]
            for row in query.data:
                for j in range(len(row)):
                    additive[ks[j]].append(row[j])
            for k in additive.keys():
                data[k]=additive[k]
        if "zs" not in data:
            xvals=data["xs"]
            del data["xs"]
            yvals=data["ys"]
            del data["ys"]
            if cmap is not None:
                data["c"]=cmap[i]
            data["label"]=thisclass
            plt.plot(xvals,yvals,**data)
        else:
            if cmap is not None:
                data["c"]=cmap[i]
            data["label"]=thisclass
            ax.plot(**data)
            ax.legend()
    if "zs" in data:
        ax.set_xlabel(xs)
        ax.set_ylabel(ys)
        ax.set_zlabel(zs)
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
