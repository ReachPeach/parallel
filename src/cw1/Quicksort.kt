package cw1

import kotlin.random.Random
import kotlin.system.measureTimeMillis
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.RecursiveAction

private fun partition(array: IntArray, low: Int, high: Int): Int {
    val pivot = array[high]
    var i = low - 1
    for (j in low..<high) {
        if (array[j] <= pivot) {
            i++
            array[i] = array[j].also { array[j] = array[i] }
        }
    }
    array[i + 1] = array[high].also { array[high] = array[i + 1] }
    return i + 1
}

fun quicksortSequential(array: IntArray, low: Int, high: Int) {
    if (low < high) {
        val pivot = partition(array, low, high)
        quicksortSequential(array, low, pivot - 1)
        quicksortSequential(array, pivot + 1, high)
    }
}

class ParQuicksortTask(private val arr: IntArray, private val low: Int, private val high: Int) : RecursiveAction() {
    override fun compute() {
        if (high - low < 64) {
            quicksortSequential(arr, low, high)
            return
        }
        val pivot = partition()
        val leftTask = ParQuicksortTask(arr, low, pivot - 1)
        val rightTask = ParQuicksortTask(arr, pivot + 1, high)
        leftTask.fork()
//        rightTask.fork()
        rightTask.compute()
        leftTask.join()
//        rightTask.join()
    }

    private fun partition(): Int {
        val pivot = arr[high]
        var i = low - 1
        for (j in low..<high) {
            if (arr[j] <= pivot) {
                i++
                arr[i] = arr[j].also { arr[j] = arr[i] }
            }
        }
        arr[i + 1] = arr[high].also { arr[high] = arr[i + 1] }
        return i + 1
    }
}

fun parQuicksort(arr: IntArray, processes: Int = 4) {
    val task = ParQuicksortTask(arr, 0, arr.size - 1)
    val pool = ForkJoinPool(processes)
    pool.invoke(task)
    pool.shutdown()
}


fun seqQuicksort(arr: IntArray) {
    quicksortSequential(arr, 0, arr.size - 1)
}

fun testImplementations() {
    val size = 100_000
    val random = Random.Default
    val arr = IntArray(size) { random.nextInt() }

    val seqSorted = arr.copyOf()
    seqQuicksort(seqSorted)

    val parSorted = arr.copyOf()
    parQuicksort(parSorted)

    val targetSorted = arr.sortedArray()
    assert(targetSorted.contentEquals(seqSorted))
    assert(targetSorted.contentEquals(parSorted))
}

fun main() {
    val size = 100_000_000
    val random = Random.Default
    val arr = IntArray(size) { random.nextInt() }
    val loopCount = 5
    val processes = 4

    println("Sample array size: $size elements")
    println("Sample loop count: $loopCount loops")
    println("Local logical processors count: ${Runtime.getRuntime().availableProcessors()}")
    println("Using $processes processes for ForkJoinPool")
    testImplementations()
    var seqTime = 0L
    repeat(loopCount) {
        val arrCopy = arr.copyOf()
        val time = measureTimeMillis {
            seqQuicksort(arrCopy)
        }
        println("Sequential measure time: $time ms")
        seqTime += time
    }
    seqTime /= loopCount
    println("Sequential sorting mean time: $seqTime ms")


    var parTime = 0L
    repeat(loopCount) {
        val arrCopy = arr.copyOf()
        val time = measureTimeMillis {
            parQuicksort(arrCopy, processes)
        }
        println("Parallel measure time: $time ms")
        parTime += time
    }
    parTime /= loopCount
    println("Parallel sorting mean time: $parTime ms")

    println("Parallel speedup is ${String.format("%.3f", seqTime.toDouble() / parTime.toDouble())}")
}

