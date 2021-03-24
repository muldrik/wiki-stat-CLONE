package ru.senin.kotlin.wiki

import org.xml.sax.Attributes
import org.xml.sax.SAXException
import org.xml.sax.helpers.DefaultHandler
import java.io.InputStream
import javax.xml.parsers.SAXParserFactory

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
    }

    private fun updateStat(currentTitle: String, currentText:String, currentSize: Int, currentTime: String) {
        pool.execute {
            val ts = getRussianWords(currentTitle.toLowerCase())
            val ws = getRussianWords(currentText.toLowerCase())
            for (t in ts)
                titles[t] = titles.getOrDefault(t, 0) + 1
            for (w in ws)
                words[w] = words.getOrDefault(w, 0) + 1
            sizes[currentSize] = sizes.getOrDefault(currentSize, 0) + 1
            currentTime.substring(0, 4).toIntOrNull()?.let { years[it] = years.getOrDefault(it, 0) + 1 }
        }
    }

    @Throws(SAXException::class)
    override fun endElement(uri: String, localName: String, qName: String) {
        if (insidePage == headers) {
            if (wasTitle && wasText && wasSize && wasTime)
                updateStat(currentTitle.toString(), currentText.toString(), currentSize, currentTime.toString())
            wasTitle = false
            wasText = false
            wasSize = false
            wasTime = false

            currentTitle = StringBuilder()
            currentText = StringBuilder()
            currentSize = 0
            currentTime = StringBuilder()
        }
        headers.removeLast()
    }

    @Throws(SAXException::class)
    override fun characters(ch: CharArray, start: Int, length: Int) {
        if (insidePageTitle == headers) {
            wasTitle = true
            currentTitle.append(String(ch, start, length))
        }
        if (insidePageRevisionText == headers) {
            wasText = true
            currentText.append(String(ch, start, length))
        }
        if (insidePageRevisionTime == headers) {
            val time = String(ch, start, length)
            wasTime = true
            currentTime.append(time)
        }
    }
}