package com.haozj.chapter02

import org.apache.flink.streaming.api.scala._

object BoundedStreamWordCount {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val inputStream = env.readTextFile("input/word.txt")
        val result: DataStream[(String, Int)] = inputStream.flatMap(_.split(" ")).map((_, 1)).keyBy(_._1).sum(1)
        result.print()
        env.execute("BoundedStreamWordCount")
    }
}
