package ru.senin.kotlin.wiki

import org.xml.sax.Attributes
import org.xml.sax.SAXException
import org.xml.sax.helpers.DefaultHandler
import java.io.InputStream
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicInteger
import javax.xml.parsers.SAXParserFactory

fun getRussianWords(s: String): List<String> =

    s.split("""[^А-Яа-я]+""".toRegex()).map { it.toLowerCase() }.filter { it.length >= 3 }

fun strCmp() = Comparator<MutableMap.MutableEntry<String, AtomicInteger>> { it1, it2 ->
    when {
        it1.key == it2.key && it1.value.get() == it2.value.get() -> 0
        it1.value.get() < it2.value.get() || (it1.value.get() == it2.value.get() && it1.key > it2.key) -> 1
        else -> -1
    }
}

fun intCmp() = Comparator<MutableMap.MutableEntry<Int, AtomicInteger>> { it1, it2 ->
    when {
        it1.key == it2.key && it1.value.get() == it2.value.get() -> 0
        it1.key < it2.key -> -1
        else -> 1
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

class SaxParser {

    @Throws(Exception::class)
    fun parse(input: InputStream) {
        val factory = SAXParserFactory.newInstance()
        val saxParser = factory.newSAXParser()
        val saxHandler = SAXHandler()
        saxParser.parse(input, saxHandler)
    }
}

val insidePage: List<String> = listOf("mediawiki", "page")
val insidePageTitle: List<String> = listOf("mediawiki", "page", "title")
val insidePageRevisionTime: List<String> = listOf("mediawiki", "page", "revision", "timestamp")
val insidePageRevisionText: List<String> = listOf("mediawiki", "page", "revision", "text")

internal class SAXHandler : DefaultHandler() {

    private var currentText = StringBuilder()
    private var currentTitle = StringBuilder()
    private var currentSize: Int = 0
    private var currentTime = StringBuilder()

    var wasText = false
    var wasTitle = false
    var wasSize = false
    var wasTime = false

    var headers: MutableList<String> = mutableListOf()

    @Throws(SAXException::class)
    override fun startElement(uri: String, localName: String, qName: String, attributes: Attributes) {
        headers.add(qName)
        if (insidePageRevisionText == headers) {
            wasText = true
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
            }
        }
        if (insidePageTitle == headers)
            wasTitle = true
    }

    private fun updateStat(currentTitle: String, currentText:String, currentSize: Int, currentTime: String) {
        val ts = getRussianWords(currentTitle.toLowerCase())
        val ws = getRussianWords(currentText.toLowerCase())
        for (t in ts)
            titles.computeIfAbsent(t) { AtomicInteger(0) }.incrementAndGet()
        for (w in ws)
            words.computeIfAbsent(w) { AtomicInteger(0) }.incrementAndGet()
        sizes.computeIfAbsent(currentSize) {AtomicInteger(0)}.incrementAndGet()
        currentTime.substring(0, 4).toIntOrNull()
            ?.let { years.computeIfAbsent(it) { AtomicInteger(0) }.incrementAndGet() }
    }

    @Throws(SAXException::class)
    override fun endElement(uri: String, localName: String, qName: String) {
        if (insidePage == headers) {
            if (wasTitle && wasText && wasSize && wasTime) {
                val title = currentTitle.toString()
                val text = currentText.toString()
                val time = currentTime.toString()
                val size = currentSize
                statPool.submit {
                    updateStat(title, text, size, time)
                }
            }
            wasTitle = false
            wasText = false
            wasSize = false
            wasTime = false

            currentTitle.clear()
            currentText.clear()
            currentSize = 0
            currentTime.clear()
        }
        headers.removeLast()
    }

    @Throws(SAXException::class)
    override fun characters(ch: CharArray, start: Int, length: Int) {
        if (insidePageTitle == headers) {
            currentTitle.append(String(ch, start, length))
        }
        if (insidePageRevisionText == headers) {
            currentText.append(String(ch, start, length))
        }
        if (insidePageRevisionTime == headers) {
            val time = String(ch, start, length)
            wasTime = true
            currentTime.append(time)
        }
    }
}