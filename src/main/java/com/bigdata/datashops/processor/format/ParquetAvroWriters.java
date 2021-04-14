package com.bigdata.datashops.processor.format;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.formats.parquet.ParquetBuilder;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

public class ParquetAvroWriters {
    public static <T extends SpecificRecordBase> ParquetWriterFactory<T> forSpecificRecord(Class<T> type,
                                                                                           CompressionCodecName compressionCodecName) {
        final String schemaString = SpecificData.get().getSchema(type).toString();
        final ParquetBuilder<T> builder =
                (out) -> createAvroParquetWriter(schemaString, SpecificData.get(), out, compressionCodecName);
        return new ParquetWriterFactory<>(builder);
    }

    public static ParquetWriterFactory<GenericRecord> forGenericRecord(Schema schema,
                                                                       CompressionCodecName compressionCodecName) {
        final String schemaString = schema.toString();
        final ParquetBuilder<GenericRecord> builder =
                (out) -> createAvroParquetWriter(schemaString, GenericData.get(), out, compressionCodecName);
        return new ParquetWriterFactory<>(builder);
    }

    public static <T> ParquetWriterFactory<T> forReflectRecord(Class<T> type,
                                                               CompressionCodecName compressionCodecName) {
        final String schemaString = ReflectData.get().getSchema(type).toString();
        final ParquetBuilder<T> builder =
                (out) -> createAvroParquetWriter(schemaString, ReflectData.get(), out, compressionCodecName);
        return new ParquetWriterFactory<>(builder);
    }

    private static <T> ParquetWriter<T> createAvroParquetWriter(String schemaString, GenericData dataModel,
                                                                OutputFile out,
                                                                CompressionCodecName compressionCodecName)
            throws IOException {
        final Schema schema = new Schema.Parser().parse(schemaString);
        return AvroParquetWriter.<T>builder(out).withSchema(schema).withDataModel(dataModel)
                       .withCompressionCodec(compressionCodecName)
                       .build();
    }

    protected ParquetAvroWriters() {
    }
}
