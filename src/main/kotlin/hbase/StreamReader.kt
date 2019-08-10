package hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import java.io.FileInputStream
import java.nio.file.Paths
import java.text.SimpleDateFormat
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamConstants

object StreamReader {
    fun stream() {
        val path = "data/enwiktionary-latest-pages-articles.xml"
        val factory = XMLInputFactory.newInstance()
        val reader = factory.createXMLStreamReader(FileInputStream(Paths.get(path).toFile()))

        val strFormat = SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'")

        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "127.0.0.1")
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        ConnectionFactory.createConnection(conf).use {
            val tableName = TableName.valueOf("wiki")
            it.getTable(tableName).use { table ->
        try {

            val buffer: MutableList<String> = mutableListOf()
            val document = mutableMapOf<String, String>()
            var count = 0

            while (reader.hasNext()) {
                when (reader.next()) {
                    XMLStreamConstants.START_ELEMENT ->
                        when (reader.localName) {
                            "page" -> document.clear()
                            "title", "timestamp", "username", "comment", "text" -> buffer.clear()
                        }
                    XMLStreamConstants.CHARACTERS ->
                        buffer.add(reader.text)
                    XMLStreamConstants.END_ELEMENT ->
                        when (reader.localName) {
                            "title", "timestamp", "username", "comment", "text" ->
                                document[reader.localName] = buffer.joinToString()
                            "revision" -> {
                                val key = document["title"]!!
                                val ts = strFormat.parse(document["timestamp"]).time

                                val text = document["text"]!!
                                val username = document["username"] ?: ""
                                val comment = document["comment"] ?: ""

                                val put = Put(key.toByte(), ts)
                                put.addColumn("text".toByte(), "".toByte(), text.toByte())
                                put.addColumn("revision".toByte(), "author".toByte(), username.toByte())
                                put.addColumn("revision".toByte(), "comment".toByte(), comment.toByte())
                                table.put(put)

                                count += 1
                                if (count % 500 == 0) {
                                    println("$count: count, $key: title")
                                }
                            }
                        }
                }
            }
        } finally {
            // 8.処理が終わったら、忘れずにXMLStreamReaderをcloseする
            if (reader != null) {
                reader.close()
            }
        }
            }
        }
    }
}

fun String.toByte() = Bytes.toBytes(this)