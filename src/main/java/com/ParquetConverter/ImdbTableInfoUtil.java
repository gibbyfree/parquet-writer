package com.ParquetConverter;

import java.util.Arrays;

import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import com.ParquetConverter.TpcDsTableInfoUtil.TableInfo;

public class ImdbTableInfoUtil {
        public static TableInfo getTableInfo(String tableName) {
                switch (tableName) {
                        case "name.basics":
                                return new TableInfo(
                                                Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("nconst")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("primaryName")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("birthYear")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("deathYear")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("primaryProfession")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("knownForTitles")
                                                                .named("name.basics"),
                                                Arrays.asList(0));
                        case "title.akas":
                                return new TableInfo(
                                                Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("titleId")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("ordering")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("title")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("region")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("language")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("types")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("attributes")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("isOriginalTitle")
                                                                .named("title.akas"),
                                                Arrays.asList(0, 1));
                        case "title.basics":
                                return new TableInfo(
                                                Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("tconst")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("titleType")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("primaryTitle")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("originalTitle")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("isAdult")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("startYear")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("endYear")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("runtimeMinutes")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("genres").named("title.basics"),
                                                Arrays.asList(0));
                        case "title.crew":
                                return new TableInfo(
                                                Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("tconst")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("directors")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("writers").named("title.crew"),
                                                Arrays.asList(0));
                        case "title.episode":
                                return new TableInfo(
                                                Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("tconst")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("parentTconst")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("seasonNumber")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("episodeNumber")
                                                                .named("title.episode"),
                                                Arrays.asList(0));
                        case "title.principals":
                                return new TableInfo(
                                                Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("tconst")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("ordering")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("nconst")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("category")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("job")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("characters")
                                                                .named("title.principals"),
                                                Arrays.asList(0, 1));
                        case "title.ratings":
                                return new TableInfo(
                                                Types.buildMessage().required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("tconst")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("averageRating")
                                                                .required(PrimitiveType.PrimitiveTypeName.BINARY)
                                                                .named("numVotes")
                                                                .named("title.ratings"),
                                                Arrays.asList(0));
                        default:
                                return null;
                }
        }
}
