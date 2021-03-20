package ru.senin.kotlin.wiki

import com.apurebase.arkenv.parse as parse
import com.apurebase.arkenv.Arkenv
import com.apurebase.arkenv.argument
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Paths
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
    println("kek")
    val fin = inputFile.inputStream()
    val `in` = BufferedInputStream(fin)
    val out = Files.newOutputStream(Paths.get(outputFileName))
    val bzIn = BZip2CompressorInputStream(`in`)
    val buffer = ByteArray(bufferSize)
    var n = 0
    while (-1 != bzIn.read(buffer).also { n = it }) {
        out.write(buffer, 0, n)
    }
    out.close()
    bzIn.close()
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
                    decompress(file, file.toString(), 8192)
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
