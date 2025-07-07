from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, MapType, ArrayType

def normalize_json_column(dataframe: DataFrame) -> DataFrame:

    def _normalize_recursive(df: DataFrame, current_prefix: str = "") -> DataFrame:
        columns_to_select = []
        for field in df.schema.fields:
            col_name = field.name
            prefixed_col_name = f"{current_prefix}{col_name}" if current_prefix else col_name

            if isinstance(field.dataType, StructType):
                exploded_df = df.withColumn(col_name, F.explode(F.col(col_name)))
                
                struct_fields_to_select = [F.col(f"{col_name}.{f.name}").alias(f.name) for f in field.dataType.fields]
                temp_struct_df = exploded_df.select(*struct_fields_to_select)

                normalized_struct_df = _normalize_recursive(temp_struct_df, f"{prefixed_col_name}_")
                
                for struct_field in normalized_struct_df.schema.fields:
                    columns_to_select.append(F.col(struct_field.name))

            elif isinstance(field.dataType, MapType):
                exploded_df = df.withColumn(col_name, F.explode(F.col(col_name)))
                
                map_key_col = F.col(f"{col_name}.key").alias(f"{prefixed_col_name}_key")
                map_value_col = F.col(f"{col_name}.value").alias(f"{prefixed_col_name}_value")

                if isinstance(field.dataType.valueType, (StructType, MapType, ArrayType)):
                    temp_map_df = exploded_df.select(map_key_col, map_value_col)
                    normalized_map_value_df = _normalize_recursive(temp_map_df, f"{prefixed_col_name}_value_")
                    for map_field in normalized_map_value_df.schema.fields:
                        columns_to_select.append(F.col(map_field.name))
                else:
                    columns_to_select.append(map_key_col)
                    columns_to_select.append(map_value_col)

            elif isinstance(field.dataType, ArrayType):
                if isinstance(field.dataType.elementType, (StructType, MapType, ArrayType)):
                    exploded_df = df.withColumn(col_name, F.explode(F.col(col_name)))
                    
                    temp_array_df = exploded_df.select(F.col(col_name).alias("element"))
                    normalized_array_element_df = _normalize_recursive(temp_array_df, f"{prefixed_col_name}_")
                    for array_field in normalized_array_element_df.schema.fields:
                        columns_to_select.append(F.col(array_field.name))
                else:
                    columns_to_select.append(F.explode(F.col(col_name)).alias(prefixed_col_name))
            else:
                columns_to_select.append(F.col(prefixed_col_name))
        
        if not columns_to_select:
            return df.sparkSession.createDataFrame([], df.schema)
        return df.select(columns_to_select)

    return _normalize_recursive(dataframe)

