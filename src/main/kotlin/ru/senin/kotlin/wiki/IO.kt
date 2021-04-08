package ru.senin.kotlin.wiki

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import java.io.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

fun decompressAndPipe(inputFile: File, bufferSize: Int) {
    val fin = inputFile.inputStream()
    val input = BufferedInputStream(fin, bufferSize)
    val bzIn = BZip2CompressorInputStream(input)
    try {
        SaxParser().parse(bzIn)
    } catch (e: Exception) {
        println("Error! ${e.message}")
        throw e
    }
}

fun printWords(outputWriter: OutputStreamWriter, s: String, words: ConcurrentHashMap<String, AtomicInteger>) {
    outputWriter.write(s)
    for (v in words.entries.sortedWith(strCmp()).take(300))
        outputWriter.write("${v.value} ${v.key}\n")
    outputWriter.write("\n")
}

fun printNumbers(outputWriter: OutputStreamWriter, s: String, nums: ConcurrentHashMap<Int, AtomicInteger>) {
    outputWriter.write(s)
    var last = -1
    for (v in nums.entries.sortedWith(intCmp())) {
        if (last != -1)
            while ((++last) < v.key)
                outputWriter.write("$last 0\n")
        outputWriter.write("${v.key} ${v.value}\n")
        last = v.key
    }
}

fun printResults(outputFileName: String) {
    val outputWriter = File(outputFileName).writer()
    printWords(outputWriter, "Топ-300 слов в заголовках статей:\n", titles)
    printWords(outputWriter, "Топ-300 слов в статьях:\n", words)
    printNumbers(outputWriter, "Распределение статей по размеру:\n", sizes)
    outputWriter.write("\n")
    printNumbers(outputWriter, "Распределение статей по времени:\n", years)
    outputWriter.close()
}