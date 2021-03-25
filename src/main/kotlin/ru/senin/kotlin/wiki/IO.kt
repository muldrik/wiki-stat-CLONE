package ru.senin.kotlin.wiki

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import java.io.*
import kotlin.concurrent.thread

fun decompressAndPipe(inputFile: File, bufferSize: Int) {
    val fin = inputFile.inputStream()
    val `in` = BufferedInputStream(fin)
    val bzIn = BZip2CompressorInputStream(`in`)
    bzIn.use {
        val buffer = ByteArray(bufferSize)
        var n: Int
        val out = PipedOutputStream()
        val pipe = PipedInputStream(out, bufferSize)

        val t1 = thread {
            SaxParser().parse(pipe)
        }
        while (-1 != it.read(buffer).also { n = it }) {
            out.write(buffer, 0, n)
        }
        out.close()
        t1.join()
    }
    if (parsingPool.taskCount == 0L) {
        //If no more xml are in queue then free one more thread for updating statistics
        statPool.maximumPoolSize+=2
        statPool.corePoolSize+=2
    }
}

fun printTitles(outputWriter: FileWriter) {
    outputWriter.write("Топ-300 слов в заголовках статей:\n")
    var cnt = 0
    for (v in titles.entries.sortedWith(cmp())) {
        if (cnt == 300)
            break
        outputWriter.write("${v.component2()} ${v.component1()}\n")
        cnt++
    }
    outputWriter.write("\n")
}

fun printWords(outputWriter: FileWriter) {
    outputWriter.write("Топ-300 слов в статьях:\n")
    var cnt = 0
    for (v in words.entries.sortedWith(cmp())) {
        if (cnt == 300)
            break
        outputWriter.write("${v.component2()} ${v.component1()}\n")
        cnt++
    }
    outputWriter.write("\n")
}

fun printYears(outputWriter: FileWriter) {
    var l = -1
    var r = 10000
    while ((++l < 10000) && years.getOrDefault(l, 0) == 0);
    while ((--r >= 0) && years.getOrDefault(r, 0) == 0);
    outputWriter.write("Распределение статей по времени:\n")
    while (l <= r) {
        outputWriter.write("$l ${years.getOrDefault(l, 0)}\n")
        l++
    }
}

fun printSizes(outputWriter: FileWriter) {
    var l = -1
    var r = 10000
    while ((++l < 10000) && sizes.getOrDefault(l, 0) == 0);
    while ((--r >= 0) && sizes.getOrDefault(r, 0) == 0);
    outputWriter.write("Распределение статей по размеру:\n")
    while (l <= r) {
        outputWriter.write("$l ${sizes.getOrDefault(l, 0)}\n")
        l++
    }
    outputWriter.write("\n")
}

fun printResults(outputFileName: String) {
    val outputWriter = FileWriter(outputFileName)
    printTitles(outputWriter)
    printWords(outputWriter)
    printSizes(outputWriter)
    printYears(outputWriter)
    outputWriter.flush()
    outputWriter.close()
}