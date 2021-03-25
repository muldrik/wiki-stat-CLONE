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
            try {
                SaxParser().parse(pipe)
            } catch (e: Exception) {
                println("Error! ${e.message}")
                throw e
            }
        }
        while (-1 != it.read(buffer).also { n = it }) {
            out.write(buffer, 0, n)
        }
        out.close()
        t1.join()
    }
}

fun printTitles(outputWriter: OutputStreamWriter) {
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

fun printWords(outputWriter: OutputStreamWriter) {
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

fun printYears(outputWriter: OutputStreamWriter) {
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

fun printSizes(outputWriter: OutputStreamWriter) {
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
    val outputWriter = File(outputFileName).writer()
    printTitles(outputWriter)
    printWords(outputWriter)
    printSizes(outputWriter)
    printYears(outputWriter)
    outputWriter.close()
}