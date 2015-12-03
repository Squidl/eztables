import math

# Given a suspected and actual value, evaluate classification
def classify(classed,actual):
    if(classed):
        if(actual):
            return "HIT"
        else:
            return "FALARM"
    else:
        if(actual):
            return "MISS"
        else:
            return "REJECT"

# Round a value with a certain precision
def roundval(val,rounder):
    return ((val+(rounder/2))//rounder)*rounder

def weighted(splits,func=(lambda x:x),weight=(lambda x:x),key=(lambda x:x)):
    total=0
    agg=0
    for i in splits:
        ele=key(i)
        currweight=weight(ele)
        agg=agg+(float(currweight)*func(ele))
        total=total+currweight
    return float(agg)/total

def entropy(splits):
    total=sum(splits)
    agg=0
    for i in splits:
        if i!=0:
            agg=agg-( (float(i)/total)*math.log(float(i)/total,2) )
    return agg

def gini(splits):
    total=sum(splits)
    agg=0
    for i in splits:
        agg=agg+((float(i)/total)*(float(i)/total))
    return 1-agg

def mixedgini(splits):
    return weighted(splits,func=gini,weight=sum)

def mixedentropy(splits):
    return weighted(splits,func=entropy,weight=sum)
