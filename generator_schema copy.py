from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, 
    TimestampType, ArrayType, BooleanType, DoubleType, LongType
)
import json
from datetime import datetime
import re

class DynamicSchemaGenerator:
   
    def __init__(self, spark):
        self.spark = spark
        
    def infer_type(self, value):
        if value is None:
            return StringType()
        elif isinstance(value, bool):
            return BooleanType()
        elif isinstance(value, int):
            return LongType()
        elif isinstance(value, float):
            return DoubleType()
        elif isinstance(value, str):
            # Verifica se é uma string que contém JSON
            if self._is_json_string(value):
                try:
                    parsed_json = json.loads(value)
                    return self.infer_type(parsed_json)
                except (json.JSONDecodeError, ValueError):
                    pass
            
            if self._is_timestamp(value):
                return TimestampType()
            return StringType()
        elif isinstance(value, list):
            if not value: 
                return ArrayType(StringType())
            element_type = self.infer_type(value[0])
            return ArrayType(element_type)
        elif isinstance(value, dict):
            return self._create_struct_from_dict(value)
        else:
            return StringType()
    
    def _is_json_string(self, value):
        """Verifica se uma string é um JSON válido"""
        if not isinstance(value, str):
            return False
        
        # Verifica se começa e termina com {/} ou [/]
        stripped = value.strip()
        if (stripped.startswith('{') and stripped.endswith('}')) or \
           (stripped.startswith('[') and stripped.endswith(']')):
            try:
                json.loads(value)
                return True
            except (json.JSONDecodeError, ValueError):
                return False
        return False
    
    def _is_timestamp(self, value):
        """Verifica se uma string é um timestamp"""
        timestamp_patterns = [
            r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+\+\d{2}:\d{2}',
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',
            r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
        ]
        return any(re.match(pattern, value) for pattern in timestamp_patterns)
    
    def _create_struct_from_dict(self, data_dict):
        fields = []
        for key, value in data_dict.items():
            field_type = self.infer_type(value)
            fields.append(StructField(key, field_type, True))
        return StructType(fields)
    
    def generate_schema_from_json(self, json_data):
        # Verifica se é uma string com JSON
        if isinstance(json_data, str):
            try:
                data = json.loads(json_data)
            except (json.JSONDecodeError, ValueError):
                # Se não conseguir fazer parse, trata como string simples
                return StringType()
        else:
            data = json_data
            
        return self.infer_type(data)
    
    def read_json_with_dynamic_schema(self, json_path_or_data, sample_fraction=0.1):
        if isinstance(json_path_or_data, str) and json_path_or_data.endswith('.json'):
            # Lê arquivo JSON
            sample_df = self.spark.read.json(json_path_or_data).sample(sample_fraction)
            sample_data = sample_df.collect()
            sample_dicts = [row.asDict(recursive=True) for row in sample_data]
            
            # Usa apenas a primeira função essencial
            if sample_dicts:
                schema = self.generate_schema_from_json(sample_dicts[0])
            else:
                schema = StructType([])
            
            return self.spark.read.schema(schema).json(json_path_or_data)
        else:
            # Processa dados JSON em memória ou string JSON
            schema = self.generate_schema_from_json(json_path_or_data)
            
            if isinstance(json_path_or_data, str):
                try:
                    data = json.loads(json_path_or_data)
                except (json.JSONDecodeError, ValueError):
                    # Se não conseguir fazer parse, trata como string simples
                    return self.spark.createDataFrame([{'value': json_path_or_data}], 
                                                     StructType([StructField('value', StringType(), True)]))
            else:
                data = json_path_or_data
                
            return self.spark.createDataFrame([data], schema)