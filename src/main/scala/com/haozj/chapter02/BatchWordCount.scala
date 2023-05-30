package com.haozj.chapter02

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
object BatchWordCount {
    def main(args: Array[String]): Unit = {
        val env = ExecutionEnvironment.getExecutionEnvironment
        val line: DataSet[String] = env.readTextFile("input/word.txt")
        val result: AggregateDataSet[(String, Int)] = line.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
        result.print()

    }
}
