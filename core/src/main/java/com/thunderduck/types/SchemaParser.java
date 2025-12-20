package com.thunderduck.types;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses schema strings into StructType objects.
 *
 * <p>Supports two formats:
 * <ul>
 *   <li>Spark DDL format: {@code struct<name:type,name2:type2>}</li>
 *   <li>JSON format: {@code {"type":"struct","fields":[{"name":"id","type":"integer","nullable":false},...]}}</li>
 * </ul>
 *
 * <p>Examples (DDL format):
 * <ul>
 *   <li>{@code struct<id:int,name:string>}</li>
 *   <li>{@code struct<tags:array<string>>}</li>
 *   <li>{@code struct<data:map<string,int>>}</li>
 * </ul>
 */
public class SchemaParser {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses a Spark schema string into a StructType.
     *
     * @param schemaStr the schema string in DDL or JSON format
     * @return the parsed StructType
     * @throws IllegalArgumentException if the schema string is invalid
     */
    public static StructType parse(String schemaStr) {
        if (schemaStr == null || schemaStr.isEmpty()) {
            throw new IllegalArgumentException("Schema string cannot be null or empty");
        }

        String trimmed = schemaStr.trim();

        // Handle JSON format: {"type":"struct","fields":[...]}
        if (trimmed.startsWith("{")) {
            return parseJsonSchema(trimmed);
        }

        // Handle struct<...> format (DDL)
        if (trimmed.toLowerCase().startsWith("struct<") && trimmed.endsWith(">")) {
            String inner = trimmed.substring(7, trimmed.length() - 1);
            return parseStructFields(inner);
        }

        // Try parsing as simple field list (fallback)
        return parseStructFields(trimmed);
    }

    /**
     * Parses a JSON schema string into a StructType.
     *
     * <p>Expected format:
     * <pre>
     * {
     *   "type": "struct",
     *   "fields": [
     *     {"name": "id", "type": "integer", "nullable": false, "metadata": {}},
     *     {"name": "name", "type": "string", "nullable": true, "metadata": {}}
     *   ]
     * }
     * </pre>
     *
     * @param jsonStr the JSON schema string
     * @return the parsed StructType
     */
    private static StructType parseJsonSchema(String jsonStr) {
        try {
            JsonNode root = objectMapper.readTree(jsonStr);
            return parseJsonStructType(root);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse JSON schema: " + e.getMessage(), e);
        }
    }

    /**
     * Parses a JSON node representing a struct type.
     */
    private static StructType parseJsonStructType(JsonNode node) {
        JsonNode fieldsNode = node.get("fields");
        if (fieldsNode == null || !fieldsNode.isArray()) {
            return new StructType(new ArrayList<>());
        }

        List<StructField> fields = new ArrayList<>();
        for (JsonNode fieldNode : fieldsNode) {
            String name = fieldNode.get("name").asText();
            boolean nullable = fieldNode.has("nullable") ? fieldNode.get("nullable").asBoolean() : true;
            DataType dataType = parseJsonDataType(fieldNode.get("type"));
            fields.add(new StructField(name, dataType, nullable));
        }

        return new StructType(fields);
    }

    /**
     * Parses a JSON node representing a data type.
     * Handles both simple types (string like "integer") and complex types (object like {"type":"array",...}).
     */
    private static DataType parseJsonDataType(JsonNode typeNode) {
        if (typeNode == null) {
            throw new IllegalArgumentException("Type node cannot be null");
        }

        // Simple type as string: "integer", "string", etc.
        if (typeNode.isTextual()) {
            return parsePrimitiveType(typeNode.asText().toLowerCase());
        }

        // Complex type as object: {"type":"array", "elementType":...}
        if (typeNode.isObject()) {
            String typeName = typeNode.get("type").asText().toLowerCase();

            switch (typeName) {
                case "array":
                    JsonNode elementType = typeNode.get("elementType");
                    return new ArrayType(parseJsonDataType(elementType));

                case "map":
                    JsonNode keyType = typeNode.get("keyType");
                    JsonNode valueType = typeNode.get("valueType");
                    return new MapType(parseJsonDataType(keyType), parseJsonDataType(valueType));

                case "struct":
                    return parseJsonStructType(typeNode);

                default:
                    // Could be a primitive type in object form
                    return parsePrimitiveType(typeName);
            }
        }

        throw new IllegalArgumentException("Unsupported type node: " + typeNode);
    }

    /**
     * Parses the inner content of a struct (field definitions).
     *
     * @param fieldsStr the comma-separated field definitions
     * @return the parsed StructType
     */
    private static StructType parseStructFields(String fieldsStr) {
        if (fieldsStr.isEmpty()) {
            return new StructType(new ArrayList<>());
        }

        List<StructField> fields = new ArrayList<>();
        List<String> fieldDefs = splitTopLevel(fieldsStr, ',');

        for (String fieldDef : fieldDefs) {
            fields.add(parseField(fieldDef.trim()));
        }

        return new StructType(fields);
    }

    /**
     * Parses a single field definition.
     *
     * @param fieldDef the field definition (e.g., "name:string" or "id:int")
     * @return the parsed StructField
     */
    private static StructField parseField(String fieldDef) {
        int colonIndex = fieldDef.indexOf(':');
        if (colonIndex == -1) {
            throw new IllegalArgumentException("Invalid field definition: " + fieldDef);
        }

        String name = fieldDef.substring(0, colonIndex).trim();
        String typeStr = fieldDef.substring(colonIndex + 1).trim();

        // All fields are nullable by default in this format
        boolean nullable = true;

        DataType dataType = parseType(typeStr);
        return new StructField(name, dataType, nullable);
    }

    /**
     * Parses a type string into a DataType.
     *
     * @param typeStr the type string
     * @return the parsed DataType
     */
    private static DataType parseType(String typeStr) {
        String normalized = typeStr.toLowerCase().trim();

        // Handle array types: array<elementType>
        if (normalized.startsWith("array<") && normalized.endsWith(">")) {
            String elementTypeStr = typeStr.substring(6, typeStr.length() - 1);
            DataType elementType = parseType(elementTypeStr);
            return new ArrayType(elementType);
        }

        // Handle map types: map<keyType,valueType>
        if (normalized.startsWith("map<") && normalized.endsWith(">")) {
            String inner = typeStr.substring(4, typeStr.length() - 1);
            int commaIndex = findTopLevelComma(inner);
            if (commaIndex == -1) {
                throw new IllegalArgumentException("Invalid map type: " + typeStr);
            }
            String keyTypeStr = inner.substring(0, commaIndex).trim();
            String valueTypeStr = inner.substring(commaIndex + 1).trim();
            DataType keyType = parseType(keyTypeStr);
            DataType valueType = parseType(valueTypeStr);
            return new MapType(keyType, valueType);
        }

        // Handle nested struct types
        if (normalized.startsWith("struct<") && normalized.endsWith(">")) {
            // Nested structs are not directly supported as DataType
            // They would need special handling
            throw new UnsupportedOperationException("Nested struct types not yet supported: " + typeStr);
        }

        // Handle primitive types
        return parsePrimitiveType(normalized);
    }

    /**
     * Parses a primitive type string.
     *
     * @param typeStr the normalized type string (lowercase)
     * @return the DataType
     */
    private static DataType parsePrimitiveType(String typeStr) {
        switch (typeStr) {
            // Integer types
            case "byte":
            case "tinyint":
                return ByteType.get();
            case "short":
            case "smallint":
                return ShortType.get();
            case "int":
            case "integer":
                return IntegerType.get();
            case "long":
            case "bigint":
                return LongType.get();

            // Floating point types
            case "float":
            case "real":
                return FloatType.get();
            case "double":
                return DoubleType.get();

            // String and boolean
            case "string":
            case "varchar":
            case "text":
                return StringType.get();
            case "boolean":
            case "bool":
                return BooleanType.get();

            // Temporal types
            case "date":
                return DateType.get();
            case "timestamp":
                return TimestampType.get();

            // Binary
            case "binary":
            case "blob":
                return BinaryType.get();

            default:
                // Handle decimal types: decimal(p,s)
                if (typeStr.startsWith("decimal(") || typeStr.startsWith("numeric(")) {
                    return parseDecimalType(typeStr);
                }
                throw new UnsupportedOperationException("Unsupported type: " + typeStr);
        }
    }

    /**
     * Parses a decimal type string.
     *
     * @param typeStr the decimal type string (e.g., "decimal(10,2)")
     * @return the DecimalType
     */
    private static DecimalType parseDecimalType(String typeStr) {
        int start = typeStr.indexOf('(');
        int end = typeStr.indexOf(')');
        if (start == -1 || end == -1) {
            // Default decimal
            return new DecimalType(10, 0);
        }

        String params = typeStr.substring(start + 1, end);
        String[] parts = params.split(",");
        int precision = Integer.parseInt(parts[0].trim());
        int scale = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 0;
        return new DecimalType(precision, scale);
    }

    /**
     * Splits a string by a delimiter, respecting nested angle brackets.
     *
     * @param str the string to split
     * @param delimiter the delimiter character
     * @return the list of parts
     */
    private static List<String> splitTopLevel(String str, char delimiter) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int start = 0;

        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '<' || c == '(') {
                depth++;
            } else if (c == '>' || c == ')') {
                depth--;
            } else if (c == delimiter && depth == 0) {
                String part = str.substring(start, i).trim();
                if (!part.isEmpty()) {
                    parts.add(part);
                }
                start = i + 1;
            }
        }

        // Add the last part
        if (start < str.length()) {
            String part = str.substring(start).trim();
            if (!part.isEmpty()) {
                parts.add(part);
            }
        }

        return parts;
    }

    /**
     * Finds the top-level comma in a string (ignoring nested angle brackets).
     *
     * @param str the string to search
     * @return the index of the comma, or -1 if not found
     */
    private static int findTopLevelComma(String str) {
        int depth = 0;
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '<' || c == '(') {
                depth++;
            } else if (c == '>' || c == ')') {
                depth--;
            } else if (c == ',' && depth == 0) {
                return i;
            }
        }
        return -1;
    }
}
