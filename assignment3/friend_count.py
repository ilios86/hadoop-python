import MapReduce
import sys
import os


mr = MapReduce.MapReduce()


def mapper(record):
    key = record[0]
    value = record[1]
    
    mr.emit_intermediate(key, value)
    

def reducer(key, list_of_values):
    total = 0
    for v in list_of_values:
        total += 1
    mr.emit((key,total))
    
print os.getcwd()
inputdata = open("./data/friends.json")
mr.execute(inputdata, mapper, reducer)