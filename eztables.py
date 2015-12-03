"""
Library or manipulating csv tables.

Author:John (Sacha) Demos
"""
from tableval import types_dict
from streamlist import StreamList, filter_where, filter_split, stream_worker
from iotables import tableinput, tableoutput, getextinp, getextout, printtableoutput

from collections import namedtuple
import StringIO

tabledef=namedtuple("tabledef",["columns","types","initial"])


class eztable:
    __slots__=("filepath","columns","types","data")
    def __init__(self,source,
                 names=None,
                 types=None,
                 alltype=None,
                 async=True,
                 closed=True):
        """Create a eztable from source"""
        self.printoutputformat=printtableoutput
        if isinstance(source,str):
            self.filepath=source
            loadformat=getextinp(source)
            source=loadformat(open(self.filepath,'rU'),names=names,types=types,alltype=alltype)
        if isinstance(source,tableinput):
            sourcedef=source.getdef()
            self.columns=sourcedef.columns
            self.types=[types_dict[x] for x in sourcedef.types]
            self.data=StreamList(source,
                                 closeafter=closed,
                                 transform=lambda x:[self.types[i](x[i].strip()) for i in range(len(x))],
                                 callback=lambda:source.close())
        elif isinstance(source,eztable):
            self.columns=[x+"" for x in source.columns]
            self.types=[types_dict[x] for x in source.types]
            self.data=StreamList([x for x in source.data])
            return
        elif isinstance(source,tabledef):
            self.columns=source.columns
            self.types=[types_dict[x] for x in source.types]
            if source.initial is None:
                self.data=StreamList(closeafter=closed)
            else:
                self.data=StreamList(source.initial,closeafter=closed)
    def __str__(self,outputformat=None):
        """Generate a string that represents this table in csv format"""
        stream=StringIO.StringIO()
        out=None
        if outputformat is None:
            out = self.printoutputformat(stream)
        else:
            out = outputformat(stream)
        out.head(self)
        for x in self.data:
            out.write(x)
        out.final(self)
        tablestr=stream.getvalue()
        out.close()
        return tablestr
    def save(self,filepath=None):
        """Save the table in the csv format"""
        if filepath is None:
            filepath=self.filepath
        if filepath is None:
            print("you must provide a filepath")
        with open(filepath,'w') as f:
            outputformat=getextout(filepath)
            f.write(self.__str__(outputformat))
    def findcol(self,name):
        """Get a column index from an index or name
        
        str name = name or index of column to look up"""
        if type( name) is int:
            return name if name<len(self.columns) else None
        else:
            return self.columns.index(name)
    def addcol(self,
               name,
               val=lambda x: None,
               typeof=None):
        """Add a column to table.
        
        str name = name of new column
        func val(x) = value of new column in row x based on row x
        type typeof = type of the new column"""
        self.columns.append(name)
        if typeof is not None:
            self.types.append(types_dict[typeof])
        else:
            self.types.append(None)
        self.data=StreamList(self.data,transform=lambda x:x+[val(x)])
        return self.findcol(name)
    def renamecol(self,namefrom,nameto):
        """Rename an already existing column
        
        str namefrom = current name of column
        str nameto = new name of column"""
        nameindex=self.findcol(namefrom)
        self.columns[nameindex]=nameto
    def typecast(self,column,typeto):
        """Change the type of a column and typecast each value
        
        str column = name of column
        type typeto = type to typecast to"""
        index=self.findcol(column)
        typeto=types_dict[typeto]
        self.types[index]=typeto
        self.transvalue(index,typeto)
    def transvalue(self,name,transform):
        """Apply a transformation on a column
        
        str name = name of column
        func transform(row) = function that transforms the value
        
        for each row : row[name]=transform(row[name])"""
        nameindex=self.findcol(name)
        rowtrans=lambda x: x[:nameindex] + [ transform(x[nameindex]) ]+ x[nameindex+1:]
        self.data=StreamList(self.data,transform=rowtrans)
    def transrows(self,transform,async=True):
        """Apply a tranformation over each row.

        func transform(row) = a function that tranforms the row."""
        self.data=StreamList(self.data,transform=transform,async=True)
    def sort(self,rule=lambda x:x[0]):
        """Sort data by sorting rule
       
        func rule(row) = sort value of row"""
        news=sorted(list(self.data), key=rule)
        self.data=StreamList(news)
    def get(self,col,val):
        colindex=self.findcol(col)
        for x in self.data:
            if x[colindex]==val:
                return x
        return None
    def get_distinct(self,col):
        ind=self.findcol(col)
        return list(set([x[ind] for x in self.data]))
    def agg(self,
            func,
            start=0,
            final=lambda x:x,
            key=lambda x:x):
        """ Calculate aggregate
        
        func func = function to use to add element to aggregate state
        func start = starting state of aggregate
        func key = function for evaluating each row
        func final = function to turn final aggregate state into value"""
        if isinstance(key,str):
            key=self.getcol_f(key)
        curr=start
        for i in self.data:
            curr=func(curr,key(i))
        return final(curr)
    def apply_agg(self,aggregate):
        """Apply an aggregate object to this table.
        
        tuple aggregate = aggregate struct to evaluate vs this table"""
        return self.agg(*aggregate)
    def count(self,rule=lambda x:True):
        """Count the rows where rule holds
        
        func rule(row) = wherether or not a row counts 
        """
        count=0
        for x in self.data:
            if rule(x):
                count=count+1
        return count
    def insert(self,values,columns=None):
        """Insert the values in values into the table.
        
        list values = a list representing a row"""
        invals=[]
        if columns is not None:
            if len(values) != len(columns):
                print("value column mismatch")
            invals = [None]*len(self.columns)
            for i in range(len(columns)):
                val=values[i]
                if i<len(self.types) and self.types[i] is not None:
                    val=self.types[i](val)
                invals[self.findcol(columns[i])]=val
        else:
            if len(values) != len(self.columns):
                print("value column mismatch")
            for i in range(len(values)):
                val=values[i]
                if i<len(self.types) and self.types[i] is not None:
                    val=self.types[i](val)
                invals.append(val)
        self.data.add(invals)
    def update(self,wherefunc,upfunc,async=True):
        self.transrows(lambda x:upfunc(x) if wherefunc(x) else x,async=async)
    def select(self,wherefunc,async=True):
        """Only keep data where wherefunc returns True
        
        func wherefunc(row) = keep row in dataset if wherefunc=True"""
        self.data=filter_where(self.data,wherefunc,async)
    def project(self,columns,generated={}):
        """Only return data for specified columns
        
        list columns = list of columns to keep
        dict generated = dictionary of columns to add e.g. NAME:GENERATE_COL(row)"""
        for x in generated.keys():
            self.addcol(x,generated[x])
        colnames=[]
        colindex=[]
        for x in columns+generated.keys():
            colindex.append(self.findcol(x))
            colnames.append(self.columns[self.findcol(x)])
        self.transrows(lambda x:[x[y] for y in colindex])
        self.columns=colnames
    def groupby(self,groupers,aggs={}):
        """Create a new table with distinct vals of attributes in groupers.
        In addition for each group aggs is calculated.

        dict groupers = a dict of COLUMN_NAME:ATTRIBUTE_GENERATION_FUNCTION(row)
                        can also be a list of columns or
                        functions to generate those columns (will be named 1, 2, 3 etc...)
        dict aggs = a dict of AGG_COLUMN_NAME:AGG_STRUCT to add aggregates or statistics to the new table"""
        if isinstance(groupers,list):
            tmp={}
            for i in range(len(groupers)):
                if isinstance(groupers[i],str):
                    tmp[groupers[i]]=self.getcol_f(groupers[i])
                elif hasattr(groupers[i],'__call__'):
                    tmp[str(i)]=i
                else:
                    print("wrong type for ele in arg groupers")
            groupers=tmp
        elif not isinstance(groupers,dict):
            print("wrong type for arg groupers")
        vals={}
        groupkeys=groupers.keys()
        grouplst=[groupers[k] for k in groupkeys]
        aggkeys=aggs.keys()
        aggcomp=comp([aggs[k] for k in aggkeys])
        for row in self.data:
            index=tuple([g(row) for g in grouplst])
            if not index in vals:
                currval=aggcomp.start
            else:
                currval=vals[index]
            vals[index]=aggcomp.func(currval,aggcomp.key(row))
        newcols=groupkeys+aggkeys
        newtypes=[None]*len(newcols)
        newtable=eztable( tabledef(newcols,newtypes,[]) )
        for key in vals.keys():
            row=list(key)+ list(aggcomp.final(vals[key]))
            newtable.insert(row)
        return newtable
    def n_trials(self,
                 stuff,
                 stats,
                 prep=None,
                 varname="var"):
        """For each variable in stuff run a test on a copy of the table.
        The variable being tested is added as column varname. Before
        the aggregates are calculated an optional prep function is run.

        list stuff = a list of variables to be passed into the tests
        dict stats = a dict of DEST_COLUMN:AGGREGATE
        func prep(var,table) = a preprocessing function to prep the table
        str varname = the name of the column for the variable in the resulting table"""
        cols=stats.keys()
        statagg=comp([stats[k] for k in cols])
        cols=[varname]+cols
        resulttable=eztable(tabledef(cols,[None]*len(cols),[]))
        for i in stuff:
            testtable=eztable(self)
            testtable.addcol(varname,lambda x:i)
            if prep is not None:
                prep(i,testtable)
            resulttable.insert([i]+list(testtable.apply_agg(statagg)))
        return resulttable

    def getcol_f(self,name):
        """Create a function that looks at the index within a row
        
        str name = name or index of column"""
        index=self.findcol(name)
        return lambda row:row[index]

    def selection(self,wherefunc,async=True):
        """Create a new table that has is a selection of the passed table.
    
        eztable table = base table
        func wherefunc = if wherefunc(row) keep row from base table in new table.
        """
        newtable=eztable(self)
        newtable.data=filter_where(self.data,wherefunc,async)
        return newtable

    def splitter(self,splitfunc,bins=2,async=True):
        """Split a table into multiple tables based on a function. Essentially
        sorts rows into bins
        
        eztable table = source table
        func splitfunc = function to determine what bin to put row in
        int bins = (2) number of bins to split into
        """
        outputs=filter_split(self.data,splitfunc,bins,async)
        results=[]
        for i in outputs:
            newtable=eztable(self)
            newtable.data=i
            results.append(newtable)
        return tuple(results)

aggregate=namedtuple("aggregate",['func','start','final','key'])

def sum(key):
    """Create a aggregate struct that will find the sum.

    func key(row) = extract a value from a row"""
    return aggregate(lambda x,y:x+y,0,lambda x:x,key)

def avg(key,weight=None):
    """Create a aggregate struct that will find an average.
    
    func key(row) = extract a value from a row
    func weight(row) = extract a weight value for a row"""
    if weight:
        return aggregate( lambda x,y:(x[0]+y[0],x[1]+y[1]) , (0,0), lambda x:x[0]/x[1], lambda x:(key(x),weight(x)) )
    else:
        return aggregate( lambda x,y:(x[0]+y,x[1]+1), (0,0), lambda x:x[0]/x[1], key )

def most(key,comp=lambda x,y:x>y,indexkey=None,keepval=False):
    """Create an aggregate struct that will find record that
    is the higest value when compared with comp, after generating
    the value with key. The function returns that value or the index
    specified with indexkey that corresponds to that record"""
    start=None
    if indexkey is None:
        func=lambda x,y: y if x is None else (y if comp(y,x) else x )
        final=lambda x:x
        newkey=key
    else:
        func=lambda x,y: y if x is None else (y if comp(y[0],x[0]) else x)
        final=lambda x:x
        if not keepval:
            final=lambda x:x[1]
        newkey=lambda x:(key(x),indexkey(x))
    return aggregate(func,start,final,newkey)

def min(key,indexkey=None):
    return most(key,comp=lambda x,y:y>x,indexkey=indexkey)
max=most

def check(current,newb,n,comp):
    if len(current)<n:
        return current+[newb]
    if comp(newb,current[-1]):
        return sorted(current[:-1]+[newb],cmp=lambda x,y:-1 if comp(x,y) else 1)
    else:
        return current

def n_most(key,n,comp=lambda x,y:x>y,indexkey=None):
    start=[]
    func=lambda x,y:check(x,y,n,comp)
    final=lambda x:x
    newkey=key
    return aggregate(func,start,final,newkey)

def comp(aggs):
    """Create an aggregate struct that will run all aggregates at once.
    
    list aggs = list of agg structs to package together"""
    func=lambda x,y: tuple( [aggs[i][0](x[i],y[i]) for i in range(len(aggs)) ])
    start=[x[1] for x in aggs]
    final=lambda x: tuple( [aggs[i][2](x[i]) for i in range(len(aggs))] )
    key=lambda x: tuple( [aggs[i][3](x) for i in range(len(aggs))] )
    return aggregate(func,start,final,key)

def whereagg(agg,wherefunc):
    func=lambda x,y:agg.func(x,y) if wherefunc(y) else x
    start=agg.start
    final=agg.final
    key=agg.key
    return aggregate(func,start,final,key)
