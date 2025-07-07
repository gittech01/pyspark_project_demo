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
        if isinstance(json_data, str):
            data = json.loads(json_data)
        else:
            data = json_data
            
        return self.infer_type(data)
    
    def generate_schema_from_sample(self, sample_data):
        if not isinstance(sample_data, list):
            sample_data = [sample_data]

        all_fields = {}
        
        for sample in sample_data:
            self._collect_fields(sample, all_fields)
        return self._build_schema_from_fields(all_fields)
    
    def _collect_fields(self, data, field_collection, prefix=""):
        """Coleta recursivamente todos os campos e seus tipos"""
        if isinstance(data, dict):
            for key, value in data.items():
                full_key = f"{prefix}.{key}" if prefix else key
                
                if isinstance(value, dict):
                    self._collect_fields(value, field_collection, full_key)
                elif isinstance(value, list) and value and isinstance(value[0], dict):
                    self._collect_fields(value[0], field_collection, full_key)
                else:
                    inferred_type = self.infer_type(value)
                    if full_key not in field_collection:
                        field_collection[full_key] = inferred_type
                    else:
                        field_collection[full_key] = self._merge_types(
                            field_collection[full_key], inferred_type
                        )
    
    def _merge_types(self, type1, type2):
        """Merge dois tipos PySpark, mantendo o mais genérico"""
        if type1 == type2:
            return type1

        type_hierarchy = {
            BooleanType(): 1,
            LongType(): 2,
            DoubleType(): 3,
            TimestampType(): 4,
            StringType(): 5
        }
    
        for spark_type, priority in type_hierarchy.items():
            if isinstance(type1, type(spark_type)) and isinstance(type2, type(spark_type)):
                return type1
        
        return StringType()
    
    def _build_schema_from_fields(self, field_collection):
        root_fields = {}
        
        for field_path, field_type in field_collection.items():
            parts = field_path.split('.')
            current_level = root_fields
            
            for i, part in enumerate(parts[:-1]):
                if part not in current_level:
                    current_level[part] = {}
                current_level = current_level[part]
            
            current_level[parts[-1]] = field_type
        
        return self._dict_to_struct(root_fields)
    
    def _dict_to_struct(self, field_dict):
        fields = []
        
        for key, value in field_dict.items():
            if isinstance(value, dict):
                field_type = self._dict_to_struct(value)
            else:
                field_type = value
            
            fields.append(StructField(key, field_type, True))
        
        return StructType(fields)
    
    def read_json_with_dynamic_schema(self, json_path_or_data, sample_fraction=0.1):
        if isinstance(json_path_or_data, str) and json_path_or_data.endswith('.json'):

            sample_df = self.spark.read.json(json_path_or_data).sample(sample_fraction)
            sample_data = sample_df.collect()
            sample_dicts = [row.asDict(recursive=True) for row in sample_data]
            schema = self.generate_schema_from_sample(sample_dicts)
            
            return self.spark.read.schema(schema).json(json_path_or_data)
        else:
            schema = self.generate_schema_from_json(json_path_or_data)
            
            if isinstance(json_path_or_data, str):
                data = json.loads(json_path_or_data)
            else:
                data = json_path_or_data
                
            return self.spark.createDataFrame([data], schema)
    
    def print_schema(self, schema, indent=0):
        """Imprime o schema de forma legível"""
        spacing = "  " * indent
        
        if isinstance(schema, StructType):
            print(f"{spacing}StructType([")
            for field in schema.fields:
                print(f"{spacing}  StructField('{field.name}', ", end="")
                if isinstance(field.dataType, (StructType, ArrayType)):
                    print()
                    self.print_schema(field.dataType, indent + 2)
                    print(f"{spacing}  , {field.nullable}),")
                else:
                    print(f"{field.dataType}(), {field.nullable}),")
            print(f"{spacing}])")
        elif isinstance(schema, ArrayType):
            print(f"{spacing}ArrayType(")
            self.print_schema(schema.elementType, indent + 1)
            print(f"{spacing})")
        else:
            print(f"{spacing}{schema}()")

# Exemplo de uso
def main():
    # Dados de exemplo (seu JSON)
    sample_json = {
        "data": [
            {
                "conference_start": "2025-04-24 21:55:01.352000+00:00",
                "conversation_end": "2025-04-24 22:08:57.287000+00:00",
                "conversation_id": "c6620a8f-bb4d-4212-9363-ae3251cf8dc8",
                "conversation_start": "2025-04-24 21:48:57.411000+00:00",
                "division_ids": ["e2345938-2a57-41cd-af2f-11e53ce634e0"],
                "media_stats_min_conversation_mos": 3.418089186880809,
                "media_stats_min_conversation_r_factor": 91.76698303222656,
                "originating_direction": "outbound",
                "participants": [
                    {
                        "participant_id": "a5dc33e4-f752-4906-8acb-eade59b49b76",
                        "user_id": "52a392c2-728d-4be4-af32-365692ab8f8f",
                        "sessions": [
                            {
                                "active_skill_ids": None,
                                "acw_skipped": None,
                                "ani": "sip:5fb6b9638ea488199a48b074+itautestdrive.orgspan.comglocalhost",
                                "media_endpoint_stats": [
                                    {
                                        "codecs": ["audio/opus"],
                                        "event_time": "2025-04-24 21:55:01.377000+00:00",
                                        "invalid_packets": None
                                    }
                                ],
                                "flow": None,
                                "metrics": [
                                    {
                                        "emit_date": "2025-04-24 21:49:00.127000+00:00",
                                        "name": "tContacting",
                                        "value": 2716
                                    }
                                ],
                                "segments": [
                                    {
                                        "segment_end": "2025-04-24 21:49:00.127000+00:00",
                                        "segment_start": "2025-04-24 21:48:57.411000+00:00"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    }
    
    # Cria o gerador de schema
    generator = DynamicSchemaGenerator()
    
    # Gera schema dinâmico
    schema = generator.generate_schema_from_json(sample_json)
    
    # Imprime o schema
    print("Schema gerado dinamicamente:")
    generator.print_schema(schema)
    
    # Cria DataFrame
    df = generator.spark.createDataFrame([sample_json], schema)
    
    # Mostra o schema e dados
    print("\nSchema do DataFrame:")
    df.printSchema()
    
    print("\nDados:")
    df.show(truncate=False)
    
    # Exemplo de como lidar com mudanças na estrutura
    print("\n" + "="*50)
    print("EXEMPLO: Dados com estrutura modificada")
    print("="*50)
    
    # Dados modificados (com novos campos)
    modified_json = {
        "data": [
            {
                "conference_start": "2025-04-24 21:55:01.352000+00:00",
                "conversation_end": "2025-04-24 22:08:57.287000+00:00",
                "conversation_id": "c6620a8f-bb4d-4212-9363-ae3251cf8dc8",
                "new_field": "novo_valor",  # Campo adicionado
                "participants": [
                    {
                        "participant_id": "a5dc33e4-f752-4906-8acb-eade59b49b76",
                        "user_id": "52a392c2-728d-4be4-af32-365692ab8f8f",
                        "additional_info": {"status": "active"},  # Novo campo aninhado
                        "sessions": [
                            {
                                "ani": "sip:5fb6b9638ea488199a48b074+itautestdrive.orgspan.comglocalhost",
                                "session_type": "voice",  # Novo campo
                                "metrics": [
                                    {
                                        "emit_date": "2025-04-24 21:49:00.127000+00:00",
                                        "name": "tContacting",
                                        "value": 2716,
                                        "unit": "ms"  # Novo campo
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    }
    
    # Gera novo schema automaticamente
    new_schema = generator.generate_schema_from_json(modified_json)
    
    print("Novo schema (adaptado automaticamente):")
    generator.print_schema(new_schema)
    
    # Cria DataFrame com novo schema
    new_df = generator.spark.createDataFrame([modified_json], new_schema)
    
    print("\nNovo DataFrame:")
    new_df.printSchema()
    
    # Para múltiplas amostras (schema mais robusto)
    samples = [sample_json, modified_json]
    robust_schema = generator.generate_schema_from_sample(samples)
    
    print("\nSchema robusto (baseado em múltiplas amostras):")
    generator.print_schema(robust_schema)

if __name__ == "__main__":
    main()