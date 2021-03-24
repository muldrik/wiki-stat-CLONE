package ru.senin.kotlin.wiki

import com.apurebase.arkenv.parse as parse
import com.apurebase.arkenv.Arkenv
import com.apurebase.arkenv.argument
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.xml.sax.Attributes
import org.xml.sax.InputSource
import org.xml.sax.SAXException
import org.xml.sax.helpers.DefaultHandler
import java.io.*
import java.lang.Thread.sleep
import javax.xml.parsers.SAXParserFactory;
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.time.measureTime

val count = AtomicInteger(0)

class Parameters : Arkenv() {
    val inputs: List<File> by argument("--inputs") {
        description = "Path(s) to bzip2 archived XML file(s) with WikiMedia dump. Comma separated."
        mapping = {
            it.split(",").map { name -> File(name) }
        }
        validate("File does not exist or cannot be read") {
            it.all { file -> file.exists() && file.isFile && file.canRead() }
        }
        defaultValue = { listOf(File("dumps/arc1.bz2")) }
    }

    val output: String by argument("--output") {
        description = "Report output file"
        defaultValue = { "statistics.txt" }
    }

    val threads: Int by argument("--threads") {
        description = "Number of threads"
        defaultValue = { 4 }
        validate("Number of threads must be in 1..32") {
            it in 1..32
        }
    }
}

lateinit var parameters: Parameters

val titles: ConcurrentHashMap<String, Int> = ConcurrentHashMap()
val words: ConcurrentHashMap<String, Int> = ConcurrentHashMap()
val years: MutableList<Int> = MutableList(10000) { 0 }
val sizes: MutableList<Int> = MutableList(10000) { 0 }

fun checkIfWordIsRussian(word: String): Boolean =
    """^[а-яА-Я]{3,}$""".toRegex().matches(word)

fun getRussianWords(s: String): List<String> =
    s.split("""[^А-Яа-я]+""".toRegex()).map { it.toLowerCase() }.filter { it.length >= 3 }

fun cmp() = Comparator<MutableMap.MutableEntry<String, Int>> { it1, it2 ->
    when {
        it1.component1() == it2.component1() && it1.component2() == it2.component2() -> 0
        it1.component2() < it2.component2() || (it1.component2() == it2.component2() && it1.component1() > it2.component1()) -> 1
        else -> -1
    }
}

fun getPow(k: Int): Int {
    var cur = 10
    for (i in 0 until 10000) {
        if (k < cur)
            return i
        cur *= 10
    }
    assert(false)
    return -1
}

/*fun writeInFile(outputFile: File, info: String) {
    outputFile.printWriter().use { out -> out.println(info) }
}*/

fun printTitles(outputWriter: FileWriter) {
    outputWriter.write("Топ-300 слов в заголовках статей:\n")
    var cnt: Int = 0
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
    while (years[++l] == 0);
    while (years[--r] == 0);
    outputWriter.write("Распределение статей по времени:\n")
    while (l <= r) {
        outputWriter.write("$l ${years[l]}\n")
        l++
    }
}

fun printSizes(outputWriter: FileWriter) {
    var l = -1
    var r = 10000
    while (sizes[++l] == 0);
    while (sizes[--r] == 0);
    outputWriter.write("Распределение статей по размеру:\n")
    while (l <= r) {
        outputWriter.write("$l ${sizes[l]}\n")
        l++
    }
    outputWriter.write("\n")
}

fun decompress(inputFile: File, outputFileName: String, bufferSize: Int) {
    val fin = inputFile.inputStream()
    val `in` = BufferedInputStream(fin)
    val bzIn = BZip2CompressorInputStream(`in`)
    var outputWriter = FileWriter(outputFileName, true)
    bzIn.use { bzIn ->
        val buffer = ByteArray(bufferSize)
        var n = 0

        val factory = SAXParserFactory.newInstance()
        val saxParser = factory.newSAXParser()
        val saxHandler = SAXHandler()
        val out = PipedOutputStream()
        val pipe = PipedInputStream(out, 8192)

        try {

            /*val input = File("dumps/wiki1.bz2.xml").inputStream()
            saxParser.parse(input, saxHandler)*/

            val t1 = thread {
                saxParser.parse(pipe, saxHandler)
            }
            while (-1 != bzIn.read(buffer).also { n = it }) {
                out.write(buffer, 0, n)
            }
            out.close()
            t1.join()
            printTitles(outputWriter)
            printWords(outputWriter)
            printSizes(outputWriter)
            printYears(outputWriter)
        } finally {
            println(count)
        }
        outputWriter.close()
    }
}

class SaxParser {

    @Throws(Exception::class)
    fun parse(output: OutputStream) {
        val factory = SAXParserFactory.newInstance()
        val saxParser = factory.newSAXParser()
        val saxHandler = SAXHandler()

        val stream = BufferedInputStream(File("kek").inputStream())
        saxParser.parse(stream, saxHandler)
    }
}

internal class SAXHandler : DefaultHandler() {

    var currentText: List<String> = listOf()
    var currentTitle: List<String> = listOf()
    var currentSize: Int = 0
    var currentTime: Int = 0

    var wasText = false
    var wasTitle = false
    var wasSize = false
    var wasTime = false

    var isInsidePage = false
    var isInsidePageTitle = false
    var isInsidePageRevisionTime = false
    var isInsidePageRevisionText = false
    var isInsidePageRevision = false

    @Throws(SAXException::class)
    override fun startElement(uri: String, localName: String, qName: String, attributes: Attributes) {
        if (qName == "page") {
            isInsidePage = true
        }
        if (isInsidePage && qName == "title") {
            isInsidePageTitle = true
        }
        if (isInsidePage && qName == "revision") {
            isInsidePageRevision = true
        }
        if (isInsidePageRevision && qName == "timestamp") {
            isInsidePageRevisionTime = true
        }
        if (isInsidePageRevision && qName == "text") {
            val n = attributes.length
            var sz = -1
            for (i in 0 until n) {
                if (attributes.getLocalName(i) == "bytes") {
                    sz = attributes.getValue(i).toInt()
                    break
                }
            }
            if (sz != -1) {
                wasSize = true
                currentSize = getPow(sz)
//                sizes[getPow(sz)]++
            }
            isInsidePageRevisionText = true
        }
    }

    @Throws(SAXException::class)
    override fun endElement(uri: String, localName: String, qName: String) {
        if (qName == "page") {
            if (wasTitle && wasText && wasSize && wasTime) {
                for (t in currentTitle)
                    titles[t] = titles.getOrElse(t, { 0 }) + 1
                for (w in currentText)
                    words[w] = words.getOrElse(w, { 0 }) + 1
                sizes[currentSize]++
                years[currentTime]++
            }
            wasTitle = false
            wasText = false
            wasSize = false
            wasTime = false

            currentTitle = listOf()
            currentText = listOf()
            currentSize = 0
            currentTime = 0

            isInsidePage = false
        }
        if (qName == "title") {
            isInsidePageTitle = false
        }
        if (qName == "revision") {
            isInsidePageRevision = false
        }
        if (qName == "timestamp") {
            isInsidePageRevisionTime = false
        }
        if (qName == "text") {
            isInsidePageRevisionText = false
        }
    }

    @Throws(SAXException::class)
    override fun characters(ch: CharArray, start: Int, length: Int) {
        if (isInsidePageTitle && length >= 3) {
            wasTitle = true
            count.incrementAndGet()
            currentTitle = getRussianWords(String(ch, start, length))
//            for (t in getRussianWords(String(ch, start, length)))
//                titles[t] = titles.getOrElse(t, { 0 }) + 1
        }
        if (isInsidePageRevisionText && length >= 3) {
            wasText = true
            currentText = getRussianWords(String(ch, start, length))
//            for (w in getRussianWords(String(ch, start, length)))
//                words[w] = words.getOrElse(w, { 0 }) + 1
        }
        if (isInsidePageRevisionTime) {
            val time = String(ch, start, length)
            val year = time.substring(0, 4).toIntOrNull()
            if (year != null) {
                wasTime = true
                currentTime = year
//                years[year]++
            }
        }
    }
}

fun main(args: Array<String>) {
    try {
        parameters = Parameters().parse(args)

        if (parameters.help) {
            println(parameters.toString())
            return
        }

        val duration = measureTime {
            val threads = mutableListOf<Thread>()
            for (file in parameters.inputs) {
                threads.add(thread {
                    decompress(file, parameters.output, 262144)
                })
            }
            threads.forEach {
                it.join()
            }
        }
        println("Time: ${duration.inMilliseconds} ms")

    } catch (e: Exception) {
        println("Error! ${e.message}")
        throw e
    }
}
