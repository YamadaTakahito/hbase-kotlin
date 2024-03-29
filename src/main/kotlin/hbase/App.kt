package hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

fun addHome() {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "127.0.0.1")
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    ConnectionFactory.createConnection(conf).use {
        val tableName = TableName.valueOf("wiki")
        it.getTable(tableName).use { table ->
            val put = Put(Bytes.toBytes("Home"))
            put.addColumn(Bytes.toBytes("text"), Bytes.toBytes(""), Bytes.toBytes("Hello World"))
            put.addColumn(Bytes.toBytes("revision"), Bytes.toBytes("author"), Bytes.toBytes("jimbo"))
            put.addColumn(Bytes.toBytes("revision"), Bytes.toBytes("comment"), Bytes.toBytes("my first edit"))
            table.put(put)
        }
    }
}

fun main(args: Array<String>) {
    StreamReader.stream()
}