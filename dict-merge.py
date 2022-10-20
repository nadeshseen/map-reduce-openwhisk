# Python code to merge dict using a single
# expression
def Merge(dict1, dict2):
    res = {}
    for key, value in dict1.items():
        if key in res.keys():
            res[key]+=value
        else:
            res[key]=value
    
    for key, value in dict2.items():
        if key in res.keys():
            res[key]+=value
        else:
            res[key]=value
    return res
     
# Driver code
dict1 = {'a': 10, 'b': 8}
dict2 = {'a': 6, 'c': 4}
dict3 = Merge(dict1, dict2)
print(dict3)