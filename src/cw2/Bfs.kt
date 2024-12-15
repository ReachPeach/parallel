package cw2

import java.util.*
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.RecursiveAction
import java.util.concurrent.RecursiveTask
import kotlin.collections.ArrayList
import kotlin.math.pow
import kotlin.system.measureTimeMillis


fun seqBfs(g: List<Node>, start: Node): List<Int> {
    val dist = ArrayList(Collections.nCopies(g.size, -1))

    val queue = LinkedList<Int>()
    queue.addFirst(start.id)
    dist[start.id] = 0

    while (queue.isNotEmpty()) {
        val v = queue.pollFirst()
        for (u in g[v].to) {
            if (dist[u] < 0) {
                dist[u] = dist[v] + 1
                queue.addLast(u)
            }
        }
    }

    return dist
}

var THRESHOLD = 1024

class ParallelUtils {
    private class ParallelForProceed(
        private val src: IntArray,
        private val l: Int,
        private val r: Int,
        private val action: (Int, Int) -> Any
    ) : RecursiveAction() {
        override fun compute() {
            if (r - l < THRESHOLD) {
                for (i in l..<r) {
                    action(src[i], i)
                }
            } else {
                val m = (r + l) / 2
                val leftTask = ParallelForProceed(src, l, m, action)
                val rightTask = ParallelForProceed(src, m, r, action)

                leftTask.fork()
                rightTask.compute()
//                rightTask.fork()
                leftTask.join()
//                rightTask.join()
            }
        }
    }

    private class ParallelScan {
        class UpAction(
            private val a: IntArray,
            private val t: IntArray,
            private val i: Int,
            private val l: Int,
            private val r: Int
        ) : RecursiveTask<Int>() {
            override fun compute(): Int {
                if (r - l < THRESHOLD) {
                    var acc = 0
                    for (j in l..<r) {
                        acc += a[j]
                    }
                    t[i] = acc
                }
//                if (r - l == 1) {
//                    t[i] = a[l]
//                }
                else {
                    val m = (r + l) / 2
                    val leftTask = UpAction(a, t, 2 * i + 1, l, m)
                    val rightTask = UpAction(a, t, 2 * i + 2, m, r)

                    leftTask.fork()
//                    rightTask.fork()
//                    t[i] = leftTask.join() + rightTask.join()
                    t[i] = rightTask.compute() + leftTask.join()
                }
                return t[i]
            }
        }

        class DownAction(
            private val a: IntArray,
            private val t: IntArray,
            private val b: IntArray,
            private val i: Int,
            private val l: Int,
            private val r: Int,
            private val s: Int
        ) : RecursiveAction() {
            override fun compute() {
                if (r - l < THRESHOLD) {
                    var acc = s
                    for (j in l..<r) {
                        b[j] = acc + a[j]
                        acc += a[j]
                    }
                }
//                if (r - l == 1) {
//                    b[l] = t[i] + s
//                }
                else {
                    val m = (r + l) / 2
                    val leftTask = DownAction(a, t, b, 2 * i + 1, l, m, s)
                    val rightTask = DownAction(a, t, b, 2 * i + 2, m, r, s + t[2 * i + 1])

                    leftTask.fork()
//                    rightTask.fork()
                    rightTask.compute()
                    leftTask.join()
//                    rightTask.join()
                }
            }
        }
    }


    companion object {
        private val pool = ForkJoinPool(4)


        fun pfor(src: IntArray, f: (Int, Int) -> Any) {
            pool.invoke(ParallelForProceed(src, 0, src.size, f))
        }

        fun map(src: IntArray, f: (Int) -> Int): IntArray {
            val out = IntArray(src.size) { -1 }
            pfor(src) { e, i ->
                out[i] = f(e)
                0
            }
            return out
        }

        fun scan(src: IntArray): IntArray {
            val t = IntArray(4 * src.size) { 0 }
            val out = IntArray(src.size) { 0 }
            pool.invoke(ParallelScan.UpAction(src, t, 0, 0, src.size))
            pool.invoke(ParallelScan.DownAction(src, t, out, 0, 0, src.size, 0))
            return out
        }

        fun filter(src: IntArray, f: (Int) -> Boolean): IntArray {
            val mapped = map(src) {
                if (f(it)) 1 else 0
            }
            val sums = scan(mapped)
            val out = IntArray(sums.last()) { 0 }

            pfor(sums) { e, i ->
                val prev = if (i == 0) 0 else sums[i - 1]
                if (e > prev) {
                    out[e - 1] = src[i]
                }
            }
            return out
        }
    }
}


fun parBfs(g_: List<Node>, start: Node): IntArray {
    val g = Array(g_.size) { g_[it].to }

    val dist = IntArray(g.size) { -1 }

    var queue = IntArray(1) { start.id }
    dist[start.id] = 0

    var nextFrontier: IntArray

    while (queue.isNotEmpty()) {
        val degrees = ParallelUtils.map(queue) {
            g[it].size
        }

        val sizes = ParallelUtils.scan(degrees)
        nextFrontier = IntArray(sizes.last()) { -1 }

        ParallelUtils.pfor(queue) { v, i1 ->
            ParallelUtils.pfor(g[v]) { u, i2 ->
                if (dist[u] == -1) {
                    dist[u] = dist[v] + 1
                    val pref = if (i1 == 0) 0 else sizes[i1 - 1]
                    nextFrontier[pref + i2] = u
                }
            }
        }

        queue = ParallelUtils.filter(nextFrontier) {
            it > 0
        }
    }
    return dist
}


fun testImplementations() {
    val dims = 4
    val cubeSize = 4
    val n = (cubeSize + 1).toDouble().pow(dims.toDouble()).toInt()

    val fabric = CubeGraphFabric(dims, cubeSize)
    val g = fabric.generateCubeGraph()

    val par = parBfs(g, g[0])
    val seq = seqBfs(g, g[0])

    for (i in 0..<n) {
        val point = fabric.mapToPoint(i)
        val target = point.sum()
        assert(target == par[i])
        assert(target == seq[i])
    }
}


fun main() {
    val loopCount = 5
    val processes = 4


    val dims = 3
    val cubeSize = 499

    println("Sample cube graph dims: $dims")
    println("Sample cube graph size: $cubeSize")
    println("Local logical processors count: ${Runtime.getRuntime().availableProcessors()}")
    println("Using $processes processes for ForkJoinPool")

    testImplementations()

    val fabric = CubeGraphFabric(dims, cubeSize)
    val g = fabric.generateCubeGraph()


    var seqTime = 0L
    repeat(loopCount) {
        val time = measureTimeMillis {
            seqBfs(g, g[0])
        }
        println("Sequential measure time: $time ms")
        seqTime += time
    }
    seqTime /= loopCount
    println("Sequential bfs mean time: $seqTime ms")

    var parTime = 0L
    repeat(loopCount) {
        val time = measureTimeMillis {
            parBfs(g, g[0])
        }
        println("Parallel measure time: $time ms")
        parTime += time
    }
    parTime /= loopCount
    println("Parallel bfs mean time: $parTime ms")

    println("Parallel speedup is ${String.format("%.3f", seqTime.toDouble() / parTime.toDouble())}")
}






