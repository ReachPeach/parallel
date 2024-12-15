package cw2

import kotlin.math.pow

class Node(val id: Int, val to: IntArray)


class CubeGraphFabric(private val dims: Int, private val cubeSize: Int) {
    private var maxInd: Int = 0

    fun mapToPoint(x: Int): List<Int> {
        val point = java.util.ArrayList<Int>()
        var t = x
        val maxSize = cubeSize + 1
        for (i in 1..dims) {
            point.add(t % maxSize)
            t /= maxSize
        }
        if (t > 0) throw IllegalArgumentException("Given id($x) is bigger than maximal id for this cube config")
        return point.reversed()
    }


    private fun getAdjacent(x: Int) = iterator {
        val point = mapToPoint(x)
        val next = point.toMutableList()
        val curSum = next.reduce { acc, x -> acc * (cubeSize + 1) + x }
        for (i in 0..<dims) {
            for (delta in listOf(-1, 1)) {
                if (next[i] + delta in 0..cubeSize) {
                    val new = curSum + delta * (cubeSize + 1).toDouble().pow((dims - i - 1).toDouble()).toInt()
                    yield(new)
                }
            }
        }
    }


    fun generateCubeGraph(): List<Node> {
        val g = ArrayList<Node>((cubeSize + 1).toDouble().pow(dims.toDouble()).toInt())

        var curId = 0
        while (true) {
            try {
                val point = mapToPoint(curId)
                val node = Node(curId, getAdjacent(curId).asSequence().toList().toIntArray())
                g.add(node)

                curId++
//                if (curId % 1_000_000 == 0) {
//                    println(curId)
//                }
            } catch (e: IllegalArgumentException) {
                maxInd = curId
                break
            }
        }
        return g
    }
}