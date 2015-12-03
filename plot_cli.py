#!/usr/bin/python
import argparse

import eztables as ez
import plottables as plt
import ast

parser=argparse.ArgumentParser(
    description="""Turn eztable files into plots. Uses plottables and matplotlib."""
    )
parser.add_argument('file',metavar='FILE',type=file,
                    help='Input file')
parser.add_argument('-x','--xval',
                    dest='xval',
                    action='store',
                    required=True,
                    help='Column to find x values in. (in format "sourcename[:rename]")')
parser.add_argument('-y','--yval',
                    dest='yval',
                    action='store',
                    required=True,
                    help='Column to find y values in.')
parser.add_argument('-z','--zval',
                    dest='zval',
                    action='store',
                    help='Column to find z values in.')
parser.add_argument('-d','--zdir',
                    dest='zdir',
                    action='store',
                    help='Direction to remove in 3d projection.')
parser.add_argument('-c','--cval',
                    dest='cval',
                    action='store',
                    help='Column to find color values in.')
parser.add_argument('-a','--autocmap',
                    dest='autocmap',
                    action='store_true',
                    help='Create a namespace colormap for nominal values.')
parser.add_argument('-g','--globals',
                    dest='globals',
                    nargs="*",
                    help='Add global settings to plot. (in format "option:value")')
parser.add_argument('--xlim',
                    dest='xlim',
                    action='store',
                    help='X axis limits in format "xmin:xmax".')
parser.add_argument('--ylim',
                    dest='ylim',
                    action='store',
                    help='Y axis limits in format "ymin:ymax".')
parser.add_argument('--hline',
                    dest='hlines',
                    nargs="*",
                    action='store',
                    help='hlines as settings dictionary.')
parser.add_argument('--vline',
                    dest='vlines',
                    nargs="*",
                    action='store',
                    help='vlines as settings dictionary.')

args=parser.parse_args()
args.file.close()
filename=args.file.name
t=ez.eztable(filename)
byrow={}
glob={}
xs=args.xval
if ":" in xs:
    splitted=xs.split(":")
    t.renamecol(splitted[0],splitted[1])
    xs=splitted[1]
byrow["xs"]=xs
t.typecast(xs,float)
ys=args.yval
if ":" in ys:
    splitted=ys.split(":")
    t.renamecol(splitted[0],splitted[1])
    ys=splitted[1]
byrow["ys"]=ys
t.typecast(ys,float)
if args.zval is not None:
    zs=args.zval
    if ":" in zs:
        splitted=zs.split(":")
        t.renamecol(splitted[0],splitted[1])
        zs=splitted[1]
    byrow["zs"]=zs
    t.typecast(zs,float)
    zdirlabel=args.zdir
    if zdirlabel==xs:
        zdirlabel="x"
    if zdirlabel==ys:
        zdirlabel="y"
    if zdirlabel==zs or zdirlabel is None:
        zdirlabel="z"
    glob["zdir"]=zdirlabel
        
if args.globals is not None:
    for val in args.globals:
        splitter=val.split(":")
        glob[splitter[0]]=splitter[1]
if args.cval is not None:
    byrow["c"]=args.cval.split(":")[-1]

colormap=None
#if args.autocmap:
#    if args.cval is None:
#        print("Need color col for colormap.")
#        exit(1)
#    colormap={}
#    ccol=t.findcol(args.cval)
#    counter=0
#    for x in t.data:
#        if not x[ccol] in colormap:
#            colormap[x[ccol]]=plt.namespacecolors[counter]
#            counter=counter+1

splot=None
splot=plt.scatter(t,byrow,glob,cmap=colormap)

if args.xlim is not None:
    splits=args.xlim.split(":")
    splot.xlim(float(splits[0]),float(splits[1]))
if args.ylim is not None:
    splits=args.ylim.split(":")
    splot.ylim(float(splits[0]),float(splits[1]))

if args.hlines is not None:
    for x in args.hlines:
        data=ast.literal_eval(x)
        data=dict(data)
        splot.axhline(**data)
if args.vlines is not None:
    for x in args.vlines:
        data=ast.literal_eval(x)
        data=dict(data)
        splot.axvline(**data)

#xpatches = [plt.patches.Patch(color='red', label=x) for x in colormap]
#xpatches = [plt.patches.Patch(color='red', label='redd') for x in [1]]
#splot.legend()

splot.show()
