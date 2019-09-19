#
# ----------------------------------------------------Imports-----------------------------------------------------------------
from pyspark.context import SparkContext, SparkConf
import json
import sys
from collections import OrderedDict
import csv
from itertools import combinations
from itertools import product
import copy
import time

#
# --------------------------------------------------Global Variables----------------------------------------------------------

spark = None
items = None
inputfile = sys.argv[3]
buckets_user = None
buckets_business = None
threshold = float(sys.argv[2])
mainThreshold = float(sys.argv[1])
partition = 2
totalSize = 0
case = 1
frequentKeyFinal = []
outputfile1 = sys.argv[4]
chunkFrequentList = []
t = None
bidList = {}


#
# ------------------------------------------------- Callback Functions---------------------------------------------------------
def removeDuplicateEntriesAfter(splitIndex, iterator):
    for x in iterator:
        yield (x[0], list(set(x[1])))


def removeDuplicateEntries(splitIndex, iterator):
    for x in iterator:
        yield ((x[0], x[1]), 1)


def phase2(splitIndex, iterator):
    global frequentKeyFinal
    temp = []

    for x in iterator:
        # yield(x[0],x[1])

        for freq in frequentKeyFinal:
            if set(freq).issubset(set(x[1])):
                yield (tuple(freq), 1)
            else:
                yield (tuple(freq), 0)


def createBuckets(splitIndex, iterator):
    for x in iterator:
        yield (x[0][0], x[0][1])


def createBuckets_case2(splitIndex, iterator):
    for x in iterator:
        yield (x[0][1], x[0][0])


def makeList(x):
    if (type(x) == tuple):
        return list(x)
    else:
        return [x]


def callSonPhase1(buckets):
    global totalSize, frequentKeyFinal, threshold, outputfile1
    totalSize = len(buckets.keys().collect())
    chunkRun = buckets.mapPartitionsWithIndex(
        lambda partition_index, iter_row: aPriori(partition_index, iter_row)).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y).map(lambda x: x[0]).map(lambda x: makeList(x))

    finalOutput = (chunkRun.collect())
    x = sorted(finalOutput, key=lambda item: (len(list(item)), list(item)))

    global frequentKeyFinal
    frequentKeyFinal = copy.copy(x)


def aPriori(partition_index, iter_row):
    global threshold, partition, candidateDict, frequentDict, partitionDictionary, totalSize, chunkFrequentList

    listTemp = []
    rowList = []

    list_bId = []
    for row in iter_row:
        rowList.append(row[1])

    chunkLength = len(rowList)

    fraction = float(chunkLength)/float(totalSize)

    chunkThreshold = (threshold*fraction)

    candidateKey = list(set(list_bId))
    candidateKeyCopy = copy.copy(candidateKey)

    # Adding singletons
    tempDict = {}
    freq = []
    for row in rowList:
        for candidate in row:
            if(tempDict.get(candidate)):
                tempDict[candidate] = tempDict[candidate]+1
            else:
                tempDict[candidate] = 1
    for keys, values in tempDict.items():
        if(tempDict[keys] >= chunkThreshold):
            freq.append(keys)
    temp = list(set(freq))
    if(len(freq) == 0):
        return (listTemp)
    else:
        chunkFrequentList.extend(freq)
        listTemp = copy.copy(temp)
    loopLength = len(freq)

    # print(chunkCandidateList, chunkFrequentList)
    # Created singles

    # Creating pairs
    # removed = list(set(candidateKeyCopy) - set(freq))
    candidateKey = list(combinations(freq, 2))
    # for removable in removed:
    #     for candidate in candidateKey:
    #         if set(removable).issubset(set(candidate)):
    #             candidateKey.remove(candidate)

    candidateKeyCopy = []
    candidateKeyCopy = copy.copy(candidateKey)

    tempDict = {}
    freq = []
    for row in rowList:
        rowset = set(row)
        for candidate in candidateKey:
            push = set(candidate) & rowset
            if(len(push) == len(candidate)):
                if(tempDict.get(candidate)):
                    tempDict[candidate] = tempDict[candidate] + 1
                else:
                    tempDict[candidate] = 1
    # for row in rowList:
    #     for candidate in candidateKeyCopy:
    #         if set(candidate).issubset(set(row)):
    #             if (tempDict.get(candidate)):
    #                 tempDict[candidate] = tempDict[candidate]+1
    #             else:
    #                 tempDict[candidate] = 1

    for keys, values in tempDict.items():
        if(tempDict[keys] >= chunkThreshold):
            freq.append(tuple(sorted(list(keys))))
    # print(len(freq), len(candidateKey))
    if(len(freq) == 0):
        return (listTemp)
    else:
        chunkFrequentList.extend(freq)
        listTemp.extend(freq)

    # (looping(chunkCandidateList, chunkFrequentList, rowList, loopLength))
    print("Working on partition : ", partition_index)
    for i in range(2, loopLength):

        candidateKey = []
        # print("Started :{0}", i)
        for k in range(0, len(freq)-1):
            for j in range(k+1, len(freq)):
                if(list(freq[k][0:i-1]) == list(freq[j][0:i-1])):
                    candidateKey.append(
                        tuple(sorted(list(set(freq[k]).union(set(freq[j]))))))
        candidateKeyCopy = copy.copy(candidateKey)

    #     chunkCandidateList.append(candidateKey)

        tempDict = {}
        freq = []
        for row in rowList:
            rowset = set(row)
            for candidate in candidateKey:
                push = set(candidate) & rowset
                if(len(push) == len(candidate)):
                    if(tempDict.get(candidate)):
                        tempDict[candidate] = tempDict[candidate] + 1
                    else:
                        tempDict[candidate] = 1

        for keys, values in tempDict.items():
            if values >= chunkThreshold:
                freq.append(tuple(sorted(list(keys))))

        if(len(freq) == 0):
            # print("Finished at: ", i)
            # mainCandidateList.append(chunkCandidateList)
            return (listTemp)
        chunkFrequentList.extend(freq)
        listTemp.extend(freq)
        # return (partition_index, listTemp)


#
# ----------------------------------------Initialization and callback function----------------------------------------------

def initialize():
    global sc, spark, items, inputfile, buckets_user, buckets_business, partition, totalSize, t, mainThreshold
    print("Initializing...")
    t = time.time()
    candidateList = []
    frequentList = []
    sc_conf = SparkConf()
    sc_conf.setAppName("Task1")
    sc_conf.setMaster('local[*]')
    sc_conf.set("spark.driver.bindAddress", "127.0.0.1")
    sc = SparkContext(conf=sc_conf)
    sc.setLogLevel("ERROR")
    csvread = sc.textFile(inputfile)
    columnName = csvread.first().split(',')
    items = csvread.map(lambda line: line.split(",")).filter(
        lambda line: (line) != columnName)

    buckets_user = items.groupByKey().mapValues(list).filter(lambda x: len(
        x[1]) > mainThreshold).mapPartitionsWithIndex(removeDuplicateEntriesAfter)
    print("Without Duplicates DOne..")
    # withoutDuplicates = checkM.mapPartitionsWithIndex(
    #     removeDuplicateEntries).groupByKey().mapValues(list)

    if (case == 1):
        # buckets_user = withoutDuplicates.mapPartitionsWithIndex(
        #     createBuckets).groupByKey().mapValues(list).filter(lambda x: len(x[1]) > mainThreshold)

        callSonPhase1(buckets_user)
        print("Initializing Phase 2.....")
        finalFreq = buckets_user.mapPartitionsWithIndex(
            lambda partition_index, iter_row: phase2(partition_index, iter_row)).reduceByKey(lambda x, y: x+y).filter(lambda x: x[1] >= threshold).map(lambda x: makeList(x[0]))

        # print((finalFreq.collect()))
        finalOutput = (finalFreq.collect())
        x = sorted(finalOutput, key=lambda item: (len(list(item)), list(item)))
        # print(x)
        printingFreq(x)

        pass
    if (case == 2):
        buckets_business = withoutDuplicates.mapPartitionsWithIndex(
            createBuckets_case2).groupByKey().mapValues(list)
        callSonPhase1(buckets_business)
        print("Initializing Phase 2.....")
        finalFreq = buckets_business.mapPartitionsWithIndex(
            lambda partition_index, iter_row: phase2(partition_index, iter_row)).reduceByKey(lambda x, y: x+y).filter(lambda x: x[1] >= threshold).map(lambda x: makeList(x[0]))

        # print((finalFreq.collect()))
        finalOutput = (finalFreq.collect())
        x = sorted(finalOutput, key=lambda item: (len(list(item)), list(item)))
        # print(x)
        printingFreq(x)

        pass

#
# ----------------------------------------- Writing Output to File -----------------------------------------


def printingFreq(x):
    global frequentKeyFinal
    with open(outputfile1, 'w') as taskaFile:
        taskaFile.write("Candidates: \n")
        begin = 1
        k = 0
        taskaFile.write("("+str(frequentKeyFinal[0]).strip('[]')+")")
        for j in frequentKeyFinal:
            if len(j) == begin:
                if begin == 1:
                    if k == 0:
                        k = k+1
                        pass
                    else:
                        taskaFile.write(", ("+str(j).strip('[]')+")")
                else:
                    taskaFile.write(", "+str(tuple(j)))
            else:
                begin = begin+1
                taskaFile.write("\n\n"+str(tuple(j)))

            # Printing x:
        taskaFile.write("\n\nFrequent Itemsets: \n")
        begin = 1
        k = 0
        taskaFile.write("("+str(x[0]).strip('[]')+")")
        for j in x:
            if len(j) == begin:
                if begin == 1:
                    if k == 0:
                        k = k+1
                        pass
                    else:
                        taskaFile.write(", ("+str(j).strip('[]')+")")
                else:
                    taskaFile.write(", "+str(tuple(j)))
            else:
                begin = begin+1
                taskaFile.write("\n\n"+str(tuple(j)))
        duration = time.time() - t
        print("Duration: "+str(duration))


#
# -------------------------------------------------- Main Function Call --------------------------------------------------
def main():
    initialize()


    # aPriori(1, listcheck)
if __name__ == "__main__":
    print("Started....")
    main()
    # print("Completed")
    pass
