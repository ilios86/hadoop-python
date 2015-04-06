import MapReduce
import sys
import os

mr = MapReduce.MapReduce()

def mapper(record):
    key = record[0]
    value = record[1]
    
    mr.emit_intermediate(key, value)
    mr.emit_intermediate(value, key)
    

def reducer(key, list_of_values):
    
    list_of_values.sort()
    if len(list_of_values) == 1 or (list_of_values[0] != list_of_values[1]):
        mr.emit((key,list_of_values[0]))
        
    prev = list_of_values[0]
    count = 1
    for v in list_of_values[1:]:
        if prev == v:
            count += 1                        
        elif prev != v and count == 1:
            mr.emit((key,v))
            prev = v
            count = 1
        else:
            prev = v
            count = 1
        
    
print os.getcwd()
inputdata = open("./data/friends.json")
mr.execute(inputdata, mapper, reducer)