import re

class operatornode:
    __slots__=("children","func")
    def __init__(self,func,childs):
        self.children=childs
        self.func=func
    def __call__(self,rowval):
        children_eval=[child(rowval) for child in self.children]
        return self.func(*children_eval)
    def isknown(self):
        for i in self.children:
            if not i.isknown():
                return False
        return True
    def apply(self,table,tablename=None,default=True):
        self.children=[child.apply(table,tablename) for child in self.children]
        return self

class basevaloperator(operatornode):
    __slots__=("val","typeof")
    def __init__(self,val,typeof=None):
        self.val=val
        self.typeof=typeof
        if not self.typeof is None:
            self.val=self.typeof(self.val)
    def __call__(self,rowval):
        return self.val
    def isknown(self):
        return True
    def apply(self,table,tablename=None,default=True):
        return self

class rowcoloperator(operatornode):
    __slots__=("name")
    def __init__(self,val):
        self.name=val
    def __call__(self,rowval):
        raise ValueError("rowcol "+self.name+" must be bound to be evaluated.")
    def isknown(self):
        return False
    def apply(self,table,tablename=None,default=True):
        nameparts=self.name.split(".")
        if len(nameparts)>1 and tablename is None:
            raise ValueError("Cannot use . operator without tablename")
        if default:
            index=table.findcol(nameparts[-1])
            typeof=None
            if table.types != None:
                typeof=table.types[index]
            return rowvaloperator(index,typeof=typeof)
        else:
            return self

class rowvaloperator(operatornode):
    __slots__=("colindex","typeof")
    def __init__(self,val,typeof=None):
        self.colindex=val
        self.typeof=typeof
    def __call__(self,rowval):
        return rowval[self.colindex]
    def isknown(self):
        return False
    def apply(seld,table,tablename=None,default=True):
        return self

baseval_operators=[
   ("(\d+.\d+)",basevaloperator,{'typeof':float}),
   ("(\d+)",basevaloperator,{'typeof':int}),
   ("#([A-Za-z0-9_]+)",rowcoloperator,{}),
   ("'([^']*)'",basevaloperator,{'typeof':str}),
   ('"([^"]*)"',basevaloperator,{'typeof':str})
   ]

minus_op=lambda x,y:x-y
plus_op=lambda x,y:x+y
div_op=lambda x,y:x/y
mult_op=lambda x,y:x*y

eq_op=lambda x,y:(1 if x==y else 0)
neq_op=lambda x,y:(0 if y==y else 0)
lt_op=lambda x,y:(1 if x<y else 0)
lteq_op=lambda x,y:(1 if x<=y else 0)
gt_op=lambda x,y:(1 if x>y else 0)
gteq_op=lambda x,y:(1 if x>=y else 0)

infix_operators=[
    ("<=",operatornode,lteq_op,{}),
    (">=",operatornode,gteq_op,{}),
    ("<",operatornode,lt_op,{}),
    (">",operatornode,gt_op,{}),
    ("!=",operatornode,neq_op,{}),
    ("==",operatornode,eq_op,{}),
    ("\-",operatornode,minus_op,{}),
    ("\+",operatornode,plus_op,{}),
    ("\/",operatornode,div_op,{}),
    ("\*",operatornode,mult_op,{}),
]

baseval_comp=[]
infix_comp=[]
def compile_parser():
    global baseval_comp
    baseval_comp=[]
    global infix_comp
    infix_comp=[]
    for i in baseval_operators:
        baseval_comp.append((re.compile("^\s*"+i[0]+"\s*$"),i[1],i[2]))
    for i in infix_operators:
        infix_comp.append((re.compile("(.*)"+i[0]+"(.*)"),i[1],i[2],i[3]))
    
compile_parser()
def parse(string):
    for i in baseval_comp:
        results=i[0].match(string)
        if results is not None:
            return i[1](results.group(1),**i[2])
    for i in infix_comp:
        results=i[0].match(string)
        if results is not None:
            sub1=parse(results.group(1))
            sub2=parse(results.group(2))
            return i[1](i[2],[sub1,sub2],**i[3])
    print("Cannot parse:"+string)
