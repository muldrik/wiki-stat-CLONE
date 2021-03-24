package ru.senin.kotlin.wiki

import com.apurebase.arkenv.Arkenv
import com.apurebase.arkenv.argument
import com.apurebase.arkenv.parse
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
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

lateinit var titles: ConcurrentHashMap<String, Int>
lateinit var words: ConcurrentHashMap<String, Int>
lateinit var years: ConcurrentHashMap<Int, Int>
lateinit var sizes: ConcurrentHashMap<Int, Int>

var pool = Executors.newFixedThreadPool(4)


fun main(args: Array<String>) {
    pool
    try {
        titles = ConcurrentHashMap()
        words = ConcurrentHashMap()
        years = ConcurrentHashMap()
        sizes = ConcurrentHashMap()
        parameters = Parameters().parse(args)

        if (parameters.help) {
            println(parameters.toString())
            return
        }

        val duration = measureTime {
            val threads = mutableListOf<Thread>()
            for (file in parameters.inputs) {
                threads.add(thread {
                    decompressAndPipe(file, 262144)
                })
            }
            threads.forEach {
                it.join()
            }
            pool.shutdown()
            printResults(parameters.output)
        }
        println("Time: ${duration.inMilliseconds} ms")

    } catch (e: Exception) {
        println("Error! ${e.message}")
        throw e
    }
}
