package com.paypal.risk

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.paypal.risk.dataset.serdes.avro.generic.GenericRecordSerdesFactory
import org.apache.avro.Schema
import org.apache.spark.sql.{Encoder, SparkSession}
import org.apache.avro.generic.GenericRecord

object Main {
  val envelopSchemaString = "{  \"type\": \"record\",  \"name\": \"GenericKafkaMessage\",  \"namespace\": \"com.rtdp.bizlogging\",  \"fields\": [{   \"name\": \"namespace\",   \"type\": \"string\",   \"default\": \"BizLogging\"  },  {   \"name\": \"dataSetName\",   \"type\": \"string\",   \"default\": \"\"  },  {      \"name\": \"schemaID\",      \"type\": \"string\",      \"doc\": \"The globally unique identifier of the schema.\",      \"default\": \"\"   },  {         \"name\": \"schemaSubject\",         \"type\": \"string\",         \"doc\": \"A subject refers to the name under which the schema is registered.\",         \"default\": \"\"   },   {       \"name\": \"schemaVersion\",       \"type\": \"string\",       \"doc\": \"A specific version of the schema registered under the schemaSubject.\",       \"default\": \"\"   },  {   \"name\": \"topic\",   \"type\": \"string\",   \"default\": \"\"  },  {   \"name\": \"tenantName\",   \"type\": \"string\",   \"default\": \"PayPal\"  },  {      \"name\": \"correlationID\",      \"type\": \"string\",      \"default\": \"\"     },  {   \"name\": \"serviceName\",   \"type\": \"string\",   \"doc\": \"The service from which this message has been published.\",   \"default\": \"UNKNOWN\"  },  {   \"name\": \"hostName\",   \"type\": \"string\",   \"doc\": \"The hostname from which this message has been published.\",   \"default\": \"UNKNOWN\"  },   {     \"name\": \"publisherColo\",     \"type\": \"string\",   \"doc\": \"The datacenter location from which this message has been published.\",     \"default\": \"UNKNOWN\"   },  {         \"name\": \"timeCreatedInMS\",         \"type\": \"long\",         \"doc\": \"epoch time the message has been generated.\",         \"default\": 0     },  {      \"name\": \"customizedHeaders\",      \"type\": {          \"type\": \"map\",          \"values\": \"string\"         },       \"doc\": \"Key Value headers for this message.\",      \"default\": {}     },     {      \"name\": \"payloadType\",      \"type\": {          \"type\": \"enum\",          \"name\": \"PayloadType\",          \"namespace\": \"\",          \"symbols\": [\"TEXT\",\"AVRO\",\"JSON\"]         },      \"doc\": \"Describe the type of message is in the 'payload' field.\",      \"default\": \"\"     },  {   \"name\": \"payload\",   \"type\": \"bytes\",   \"doc\": \"The serialized bytes of the message. Refer to 'payloadType' to know its type.\"  }  ] }";
  val avroRecordDeserializer = new GenericRecordSerdesFactory(new Schema.Parser().parse(envelopSchemaString)).buildDeserializer

  val windowSize = "1 minutes"

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    implicit val encoder: Encoder[GenericRecord] = org.apache.spark.sql.Encoders.kryo[GenericRecord]

    import spark.implicits._

    val messageDs = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
//      .option("kafka.bootstrap.servers", "lvskafka1.lvs.paypal.com:9092,lvskafkamm01.lvs.paypal.com:9092")
      .option("subscribe", "rtdp.bizlogging.highqualitydata")
      .load()
      .select($"value".as[Array[Byte]])
      .map(row => {
        val genericRecord = avroRecordDeserializer.deserialize(row).asInstanceOf[GenericRecord]
        genericRecord
      })
//      .filter(_._1.equals("biz-availability"))
//      .selectExpr("bizMessage.hostName", "bizMessage.timeCreatedInMS")
//      .map(r => HostBaseTimestamp(r.getString(0), new Timestamp(r.getString(1).toLong)))
//
//    val messageCount = messageDs
//      .distinct()
//      .groupBy(window($"timestamp", windowSize, windowSize), $"hostname")
//      .count()
//
    val query = messageDs
      .writeStream
      .outputMode("Update")
//      .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
      .option("truncate", false)
      .format("console")
      .start()

    query.awaitTermination()
  }

  case class HostBaseTimestamp(hostName: String, timestamp: Timestamp)
}
