package ru.senin.kotlin.wiki

import com.apurebase.arkenv.Arkenv
import com.apurebase.arkenv.argument
import com.apurebase.arkenv.parse
import java.io.File
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.time.measureTime


class Parameters : Arkenv() {
    val inputs: List<File> by argument("--inputs") {
        description = "Path(s) to bzip2 archived XML file(s) with WikiMedia dump. Comma separated."
        mapping = {
            it.split(",").map { name -> File(name) }
        }
        validate("File does not exist or cannot be read") {
            it.all { file -> file.exists() && file.isFile && file.canRead() }
        }
        defaultValue = { listOf(File("dumps/wiki.bz2")) }
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

val titles: ConcurrentHashMap<String, AtomicInteger> = ConcurrentHashMap()
val words: ConcurrentHashMap<String, AtomicInteger> = ConcurrentHashMap()
val years: ConcurrentHashMap<Int, AtomicInteger> = ConcurrentHashMap()
val sizes: ConcurrentHashMap<Int, AtomicInteger> = ConcurrentHashMap()

lateinit var parsingPool: ThreadPoolExecutor
lateinit var statPool: ThreadPoolExecutor

const val bufferSize = 16384*16*16

fun main(args: Array<String>) {
    try {
        titles.clear()
        words.clear()
        years.clear()
        sizes.clear()
        parameters = Parameters().parse(args)

        if (parameters.help) {
            println(parameters.toString())
            return
        }

        val duration = measureTime {
            val fileCount = parameters.inputs.size

            parsingPool = ThreadPoolExecutor(minOf(parameters.threads, fileCount), minOf(parameters.threads, fileCount),
                0L, TimeUnit.MILLISECONDS, LinkedBlockingQueue())
            //Every thread from this pool invokes one more thread
            statPool = ThreadPoolExecutor(parameters.threads, parameters.threads,
                0L, TimeUnit.MILLISECONDS, LinkedBlockingQueue())
            //All threads that are not parsing the files directly help to update statistics

            for (file in parameters.inputs) {
                parsingPool.execute {
                    decompressAndPipe(file, bufferSize)
                }
            }
            parsingPool.shutdown()
            parsingPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)
            statPool.shutdown()
            statPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)
        }
        println("Time: ${duration.inMilliseconds} ms")
        val printDuration = measureTime {
            printResults(parameters.output)
        }
        println("Output time: $printDuration")


    } catch (e: Exception) {
        println("Error! ${e.message}")
        throw e
    }
}
