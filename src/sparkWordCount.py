# -*- coding: utf-8 -*-

from pyspark import SparkContext
import re
from math import ceil
import sys
from operator import add
sc = SparkContext("local[*]", "word count")
sc.setLogLevel("WARN")

def getWordCountsList(sc, filePath):
    words = sc.textFile(filePath).flatMap(
    lambda line:
        # list(re.sub(r'[^\w]', '', line).lower())
        re.sub(r'[^\w]', ' ', line).lower().split()
    )
    wordCounts = words.map(lambda word: (word, 1))\
        .reduceByKey(add)\
        .sortBy(keyfunc=lambda x:x[1], ascending=False)
    return list(wordCounts.collect()), words.count()

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
        frequency = words[rank][1]
        string = f"\t{rank}\t{word:20}\t{category:10}\t{frequency}\n"
        sys.stdout.buffer.write(string.encode('utf-8'))

def printBorder():
    print(f"\trank\t{'word':20}\tcategory\tfrequency")
    print("\t----------------------------------------------------------")

filePath = sys.argv[1]
wordCountList, totalNum = getWordCountsList(sc, filePath)
popularWordsRange, commonWordsRange, rareWordsRange = get3Ranges(len(wordCountList))
printBorder()
outputFormat(wordCountList, popularWordsRange, "popular", totalNum)
# print(f"total number of words: {totalNum}")
# outputFormat(wordCountList, commonWordsRange, "common", totalNum)
# outputFormat(wordCountList, rareWordsRange, "rare", totalNum)
