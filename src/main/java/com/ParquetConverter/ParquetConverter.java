package com.ParquetConverter;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import com.ParquetConverter.TpcDsTableInfoUtil.TableInfo;

import org.apache.parquet.schema.PrimitiveType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class ParquetConverter {

        public static void main(String[] args) {
                if (args.length < 3) {
                        System.err.println(
                                        "Usage: ParquetConverter [imdb|tpcsds] <inputDir> <outputDir> [compression] [--perf]");
                        System.exit(1);
                }

                String mode = args[0];
                String inputDir = args[1];
                String outputDir = args[2];

                String compression = "";
                boolean runtimes = false;
                for (int i = 3; i < args.length; i++) {
                        if (args[i].equals("--perf")) {
                                runtimes = true;
                        } else {
                                compression = args[i];
                        }
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

                String pattern = mode.equals("tpcds") ? ".dat" : ".tsv";
                File[] inputFiles = new File(inputDir).listFiles((dir, name) -> name.endsWith(pattern));

                if (inputFiles == null) {
                        System.err.println("No input files found in directory: " + inputDir);
                        return;
                }

                // Convert to parquet and time compression
                for (File inputFile : inputFiles) {
                        String tableName = inputFile.getName().replace(pattern, "");
                        TableInfo tableInfo = mode.equals("tpcds") ? TpcDsTableInfoUtil.getTableInfo(tableName)
                                        : ImdbTableInfoUtil.getTableInfo(tableName);

                        if (tableInfo == null) {
                                System.err.println("Skipping unknown table: " + tableName);
                                continue;
                        }

                        String outputPath = new File(outputDir, tableName + ".parquet").getPath();

                        // Skip if output file already exists
                        if (new File(outputPath).exists()) {
                                System.err.println("Skipping existing file: " + outputPath);
                                continue;
                        }

                        long start = System.nanoTime();
                        convertFile(inputFile.getPath(), outputPath, tableInfo, codec);
                        long end = System.nanoTime();

                        if (runtimes)
                                System.out.println(
                                                "Compressing " + tableName + " took: " + (end - start) / 1e6 + " ms");
                }

                // Read files and time decompression
                if (runtimes) {
                        for (File inputFile : inputFiles) {
                                String parquetFile = new File(outputDir,
                                                (inputFile.getName().replace(pattern, ".parquet")))
                                                .getPath();
                                try {
                                        long start = System.nanoTime();
                                        readFile(parquetFile);
                                        long end = System.nanoTime();
                                        System.out.println(
                                                        "Decompressing " + parquetFile + " took: " + (end - start) / 1e6
                                                                        + " ms");
                                } catch (IOException e) {
                                        e.printStackTrace();
                                }
                        }
                }
        }

        private static void readFile(String parquetPath) throws IOException {
                Configuration conf = new Configuration();
                Path path = new Path(parquetPath);

                int rowCount = 0;
                try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path).withConf(conf)
                                .build()) {
                        Group group;
                        while ((group = reader.read()) != null) {
                                rowCount++; // just count rows to prevent heap overload
                        }
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

                        String delimiter = inputPath.endsWith(".tsv") ? "\t" : "|";
                        try (BufferedReader br = new BufferedReader(new FileReader(inputPath))) {
                                String line;
                                while ((line = br.readLine()) != null) {
                                        String[] fields = line.split(Pattern.quote(delimiter), -1);

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