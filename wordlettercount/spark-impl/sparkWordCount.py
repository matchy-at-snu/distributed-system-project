# -*- coding: utf-8 -*-

from pyspark import SparkContext
import re
from math import ceil
import sys
from operator import add, le

# borrowed randomly from internet, for printing non-ascii character
import codecs
sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

import time
startTime = time.time()

sc = SparkContext("local[*]", "word count")
sc.setLogLevel("WARN")

def wordSep(line):
    filter1 = re.compile(r"[^\w\'-]|[\d_]", re.U)
    line = re.sub(filter1, ' ', line)
    filter2 = re.compile(r"\\b-|-\\b|-{2,}", re.U)
    line = re.sub(filter2, ' ', line)
    filter3 = re.compile(r"\\b\'|\'\\b|\'{2,}", re.U)
    return re.sub(filter3, ' ', line).lower().split()

def getWordCountsList(sc, filePath):
    words = sc.textFile(filePath).flatMap(wordSep)
    wordCounts = words.map(lambda word: (word, 1))\
        .reduceByKey(add)\
        .sortBy(keyfunc=lambda x:x[1], ascending=False)
    return list(wordCounts.collect()), words.count()

def getLetterCountsList(sc, filePath):
    letters = sc.textFile(filePath).flatMap(
        lambda line:
            list(re.sub(r'[\W\d_]', '', line).lower())
    )
    letterCounts = letters.map(lambda letter: (letter, 1))\
        .reduceByKey(add)\
        .sortBy(keyfunc=lambda x:x[1], ascending=False)
    return list(letterCounts.collect()), letters.count()

def get3Ranges(wordCountSize):
    wordCountSize_5p = int(ceil(wordCountSize*0.05))
    commonStartIndex = int(wordCountSize/2)-int(wordCountSize_5p/2)
    popularWordsRange = (0,wordCountSize_5p)
    commonWordsRange = (commonStartIndex,commonStartIndex+wordCountSize_5p)
    rareWordsRange = (wordCountSize-wordCountSize_5p,wordCountSize)
    return (popularWordsRange, commonWordsRange, rareWordsRange)

def outputFormat(words, wordsRange, categoryString, totalNum):
    for rank in range(wordsRange[0], wordsRange[1]):
        word = words[rank][0]
        category = categoryString
        frequency = words[rank][1]/totalNum
        string = ("\t"+str(rank).ljust(10)+"\t"+word.ljust(20)+"\t"
            +category.ljust(10)+"\t"+str(frequency).ljust(20)+"\n")
        sys.stdout.write(string)

def printBorder(type):
    print(f"\t{'rank':10}\t{str(type):20}\t{'category':10}\tfrequency")
    print("\t----------------------------------------------------------")

filePath = sys.argv[1]
wordCountList, wordNum = getWordCountsList(sc, filePath)
popularWordsRange, commonWordsRange, rareWordsRange = get3Ranges(len(wordCountList))
printBorder("word")
outputFormat(wordCountList, popularWordsRange, "popular", wordNum)
outputFormat(wordCountList, commonWordsRange, "common", wordNum)
outputFormat(wordCountList, rareWordsRange, "rare", wordNum)

print("\n\n")

letterCountsList, letterNum = getLetterCountsList(sc, filePath)
popularLetterRange, commonLetterRange, rareLetterRange = get3Ranges(len(letterCountsList))
printBorder("letter")
outputFormat(letterCountsList, popularLetterRange, "popular", letterNum)
outputFormat(letterCountsList, commonLetterRange, "common", letterNum)
outputFormat(letterCountsList, rareLetterRange, "rare", letterNum)

finishTime = time.time()
print("execution time: %s seconds" %(finishTime-startTime))