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

fun decompress(inputFile: File, outputFileName: String, bufferSize: Int) {
    val fin = inputFile.inputStream()
    val `in` = BufferedInputStream(fin)
    val bzIn = BZip2CompressorInputStream(`in`)
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
        }
        finally {
            println(count)
        }
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


    var isInsidePage = false
    var isInsidePageTitle = false


    @Throws(SAXException::class)
    override fun startElement(uri: String, localName: String, qName: String, attributes: Attributes) {
        if (qName.equals("page", ignoreCase = true)) {
            isInsidePage = true
        }
        if (isInsidePage && qName.equals("title", ignoreCase = true)) {
            isInsidePageTitle = true
        }
    }

    @Throws(SAXException::class)
    override fun endElement(uri: String, localName: String, qName: String) {
        if (isInsidePage && qName.equals("title", ignoreCase = true)) {
            isInsidePageTitle = false
        }
        if (qName.equals("page", ignoreCase = true)) {
            isInsidePage = false
        }
        if (qName.equals("mediawiki", ignoreCase = true)) {
        }
    }

    @Throws(SAXException::class)
    override fun characters(ch: CharArray, start: Int, length: Int) {
        if (isInsidePageTitle && length > 0) {
            count.incrementAndGet()
            val title = String(ch, start, length)
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
                threads.add(thread{
                    decompress(file, file.toString().plus(".xml"), 262144)
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
