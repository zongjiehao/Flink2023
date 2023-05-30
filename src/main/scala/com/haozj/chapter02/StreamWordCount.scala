package com.haozj.chapter02

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
    def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        val tool: ParameterTool = ParameterTool.fromArgs(args)
        val hostname: String = tool.get("hostname")
        val port: Int = tool.getInt("port")
        val inputStream: DataStream[String] = env.socketTextStream(hostname,port)
        val result: DataStream[(String, Int)] = inputStream.flatMap(_.split(" ")).map((_, 1)).keyBy(_._1).sum(1)
        result.print()
        env.execute("BoundedStreamWordCount")
    }
}
