#!/usr/bin/python
import sys
import argparse
import re
from pickle import loads
from eztables import eztable, types_dict
from ezname import avail
import mathval

def makeparser(withfile=True):
    parser = argparse.ArgumentParser(
        description="""Manipulate CSV files.
In each CLAUSE, column names are refered to by $NAME. $i is interptered as the ith column (where i is an int). Thus, NAME cannot start with a number.""")
    if withfile:
        parser.add_argument('file', metavar='FILE', type=file, #nargs='+',
                            help='File to manipulate data from.')
    parser.add_argument('-p','--print', dest='printout', action='store_true',
                        help='print results.')
    parser.add_argument('-n','--nameless', action='store_true',
                        help='remove the column names from the output')
    parser.add_argument('-c','--columns', action='store_true',
                        help='print column names')
    parser.add_argument('-t','--types',action='store', metavar='CLAUSE', type=str,
                        help='typecast columns.')
    parser.add_argument('-r','--rename',action='store', metavar='CLAUSE', type=str,
                        help='rename columns. renaming a column to "" has no effect.')
    parser.add_argument('-s','--select',action='store', metavar='CLAUSE', type=str,
                        help='return columns listed in clause.')
    parser.add_argument('-a','--add_col',action='store', metavar='CLAUSE', type=str,
                        help='return columns listed in clause.')
    parser.add_argument('-w','--where', action='store', metavar='CLAUSE', type=str,
                        help='only return rows where CLAUSE is true')
    parser.add_argument('--overwrite', action='store_true',
                        help='overwrite the file with output')
    parser.add_argument('-o', action='store',dest='output',
                        help='write to output')
    parser.add_argument('-i','--iterate', action='store_true',
                        help='save without overwriting. Will also open latest file.')
    parser.add_argument('--shell', action='store_true',
                        help='open shell.')
    parser.add_argument('--history', action='store_true',
                       help='print history in shell mode')
    parser.add_argument('-q','--quit', action='store_true',
                        help='quit shell.')
    return parser

parser=makeparser()
args = parser.parse_args()
args.file.close()
basefilename=args.file.name
filename=basefilename
if(args.iterate):
    filename=avail(filename,last=True)
table=eztable(filename)

if args.nameless:
    table.columns=[None]*len(table.columns)

def main(table,args,shell=False):
    altered=False
    if(args.types is not None):
        types=[x.strip() for x in args.types.split(",")]
        for x in range(len(types)):
            table.typecast(x,types_dict[types[x]])
        altered=True
    if(args.rename is not None):
        names=args.rename.split(",")
        for x in range(len(names)):
            colindex=table.findcol(x)
            if colindex is not None:
                table.renamecol(colindex,names[x])
        altered=True
    if args.add_col is not None:
        for i in args.add_col.split(","):
            cols = i.split(":")
            if len(cols)<2:
                print "invalid column : "+i
            name=cols[0]
            exp=":".join(cols[1:])
            func=mathval.parse(exp)
            table.addcol(name,func.apply(table))
            altered=True
    if(args.where is not None):
        func=mathval.parse(args.where)
        func=func.apply(table)
        table.select(func)
        altered=True
    if(args.select is not None):
        table.project(args.select.split(","))
        altered=True

    if(args.overwrite):
        table.save()
        print("Saved to: "+table.filepath)
        if(args.printout):
            print(table)
    elif(args.iterate):
        if altered or shell:
            newname=avail(basefilename)
            table.filepath=newname
            table.save()
            print("Saved to: "+table.filepath)
            if(args.printout):
                print(table)
    elif(args.output is not None):
        table.filepath=args.output
        table.save()
        print("Saved to: "+table.filepath)
        if(args.printout):
            print(table)
    elif(args.printout or (not args.columns and not args.quit and args.types!=None )):
        print(table)
    if args.columns or args.types!=None:
        print(",".join(table.column_str()))

def nostring_split(firststring,chars=[" "],string_lim=["'",'"'],end_lim=[";"]):
    string=firststring
    stack=[]
    curse=0
    while True:
        if len(string)-curse <= 0:
            return None,None
        for lim in end_lim:
            if string[curse:curse+len(lim)]==lim:
                if curse>0:
                    stack.append(string[:curse])
                return stack,string[curse+1:]
        for c in chars:
            if string[curse:curse+len(c)]==c:
                stack.append(string[:curse])
                string=string[curse+1:]
                curse=-1
        for lim in string_lim:
            if string[curse:curse+len(lim)]==lim:
                if curse>0:
                    stack.append(string[:curse])
                nexter=string[curse+1:].find(lim)
                if nexter==-1:
                    return None,None
                stack.append(string[curse+1:curse+1+nexter])
                string=string[curse+2+nexter:]
                curse=-1
        curse=curse+1

if args.shell:
    parser=makeparser(withfile=False)
    history=[]
    rest=""
    while True:
        argsplit=None
        line=rest
        while argsplit is None:
            if line.find(";")==-1:
                if line=="":
                    print(">"),
                else:
                    print("~"),
                readline=sys.stdin.readline()
                if readline=="":
                    readline="-q;"
                line=line+readline.strip()
            argsplit,rest=nostring_split(line)
        args=parser.parse_args(argsplit)
        history.append(line)
        main(table,args,shell=True)
        if args.history:
            for histline in history:
                print(histline)
        if args.quit:
            break
else:
    main(table,args)
