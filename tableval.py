class tabletype:
    """A wrapper for python types so that they can be parsed or serached.
    The pytype does not need to be a python type but is assumed to be
    callable (to typecast) and to identify values with isinstance.
    The label and aliases are used for str_>type parsing."""
    __slots__=("label","pytype","aliases")
    def __init__(self,label,pytype,aliases=[],call=None):
        self.label=label
        self.pytype=pytype
        self.aliases=[]+aliases
        if isinstance(pytype,type):
            self.__call__=self.pytype.__call__
        if not call is None:
            self.__call__=call

class typeset:
    """Table type collection class. Used to create
    dictionaries of types."""
    __slots__=("types","nonetype")
    def __init__(self):
        self.types=[]
        self.nonetype=tabletype("none",
                                None,
                                call=lambda x:x)
    def __getitem__(self,x):
        """Get an item based on a key x.
        
        x is a string: find a type whose label is x
        x is a type: a tabletype whose python type is x
        x is a tabletype: that tabletype if its in the dict"""
        if x is None:
            return self.nonetype
        if isinstance(x,tabletype):
            if x in self.types:
                return x
            elif x.pytype is None:
                return self.nonetype
        if isinstance(x,type):
            for i in self.types:
                if i.pytype==x:
                    return i
        if isinstance(x,str):
            for i in self.types:
                if i.label==x:
                    return i
            for i in self.types:
                if x in i.aliases:
                    return i
        print("cannot find type")

types_dict=None
def default_types_dict():
    """Set up with a types_dict with the default types
    and labels/aliases."""
    global types_dict
    types_dict=typeset()
    types_dict.types.append(tabletype(
            "int",
            int))
    types_dict.types.append(tabletype(
            "float",
            float,
            aliases=["double"]))
    types_dict.types.append(tabletype(
            "str",
            str))
default_types_dict()
