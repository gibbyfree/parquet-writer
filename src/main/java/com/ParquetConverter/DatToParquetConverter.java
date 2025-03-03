package com.ParquetConverter;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import com.ParquetConverter.TableInfoUtil.TableInfo;
import static com.ParquetConverter.TableInfoUtil.getTableInfo;

import org.apache.parquet.schema.PrimitiveType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class DatToParquetConverter {

        public static void main(String[] args) {
                if (args.length < 2) {
                        System.err.println("Usage: DatToParquetConverter <inputDir> <outputDir>");
                        System.exit(1);
                }

                String inputDir = args[0];
                String outputDir = args[1];

                String compression = "";
                if (args.length == 3) {
                        compression = args[2];
                }

                CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
                switch (compression) {
                        case "gzip":
                                codec = CompressionCodecName.GZIP;
                                break;
                        case "snappy":
                                codec = CompressionCodecName.SNAPPY;
                                break;
                        case "lzo":
                                codec = CompressionCodecName.LZO;
                                break;
                        default:
                                break;
                }

                File[] datFiles = new File(inputDir).listFiles((dir, name) -> name.endsWith(".dat"));

                if (datFiles == null) {
                        System.err.println("No .dat files found in input directory: " + inputDir);
                        return;
                }

                for (File datFile : datFiles) {
                        String tableName = datFile.getName().replace(".dat", "");
                        TableInfo tableInfo = getTableInfo(tableName);

                        if (tableInfo == null) {
                                System.err.println("Skipping unknown table: " + tableName);
                                continue;
                        }

                        String outputPath = new File(outputDir, tableName + ".parquet").getPath();
                        convertFile(datFile.getPath(), outputPath, tableInfo, codec);
                }
        }

        private static void convertFile(String inputPath, String outputPath, TableInfo tableInfo,
                        CompressionCodecName codec) {
                MessageType schema = tableInfo.getSchema();
                List<Integer> pkIndices = tableInfo.getPkIndices();

                Configuration conf = new Configuration();
                GroupWriteSupport.setSchema(schema, conf);

                try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(outputPath))
                                .withConf(conf)
                                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                                .withCompressionCodec(codec)
                                .withRowGroupSize(128 * 1024 * 1024)
                                .withPageSize(512 * 1024)
                                .withValidation(true)
                                .build()) {

                        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

                        try (BufferedReader br = new BufferedReader(new FileReader(inputPath))) {
                                String line;
                                while ((line = br.readLine()) != null) {
                                        String[] fields = line.split("\\|", -1);

                                        // Check primary key fields
                                        boolean pkValid = true;
                                        for (int pkIndex : pkIndices) {
                                                if (pkIndex >= fields.length || fields[pkIndex].isEmpty()) {
                                                        pkValid = false;
                                                        break;
                                                }
                                        }
                                        if (!pkValid) {
                                                System.err.println("Skipping line with missing PK: " + line);
                                                continue;
                                        }

                                        Group group = groupFactory.newGroup();
                                        boolean valid = true;

                                        for (int i = 0; i < schema.getFields().size(); i++) {
                                                String fieldName = schema.getFieldName(i);
                                                String fieldValue = i < fields.length ? fields[i] : "";
                                                PrimitiveType type = schema.getType(fieldName).asPrimitiveType();

                                                if (fieldValue.isEmpty()) {
                                                        continue; // Skip optional fields
                                                }

                                                try {
                                                        switch (type.getPrimitiveTypeName()) {
                                                                case INT32:
                                                                        group.append(fieldName,
                                                                                        Integer.parseInt(fieldValue));
                                                                        break;
                                                                case INT64:
                                                                        group.append(fieldName,
                                                                                        Long.parseLong(fieldValue));
                                                                        break;
                                                                case BINARY:
                                                                        group.append(fieldName, fieldValue);
                                                                        break;
                                                                case DOUBLE:
                                                                        group.append(fieldName,
                                                                                        Double.parseDouble(fieldValue));
                                                                        break;
                                                                default:
                                                                        System.err.println("Unhandled type: "
                                                                                        + type.getPrimitiveTypeName()
                                                                                        + " for field " + fieldName);
                                                                        valid = false;
                                                        }
                                                } catch (NumberFormatException e) {
                                                        System.err.println("Error parsing field " + fieldName
                                                                        + " with fieldvalue : " + fieldValue
                                                                        + " from line: " + line);
                                                        valid = false;
                                                }
                                                if (!valid)
                                                        break;
                                        }

                                        if (valid) {
                                                writer.write(group);
                                        }
                                }
                        }

                        System.out.println("Converted: " + inputPath + " to " + outputPath);
                } catch (IOException e) {
                        e.printStackTrace();
                }
        }
}