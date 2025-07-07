from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, ArrayType, MapType
from pyspark.sql.functions import col, posexplode_outer, explode_outer, concat_ws


class NormalizeJSON:
    """
    Classe simples para achatar completamente campos aninhados em DataFrames do PySpark.
    """
    
    def __init__(self, separator: str = "_", max_depth: int = 50):
        self.separator = separator
        self.max_depth = max_depth
    
    def flatten(self, df: DataFrame) -> DataFrame:
        if not isinstance(df, DataFrame):
            raise ValueError("O parâmetro 'df' deve ser um DataFrame do PySpark")
        
        current_df = df
        iteration = 0
        
        while iteration < self.max_depth:
            has_nested = self._has_nested_structures(current_df)
            
            if not has_nested:
                break
            
            current_df = self._flatten_all_structs(current_df)
            current_df = self._flatten_all_arrays(current_df)
            current_df = self._flatten_all_maps(current_df)
            
            iteration += 1
        
        current_df = self._clean_column_names(current_df)
        
        return current_df
    
    def _has_nested_structures(self, df: DataFrame) -> bool:
        """Verifica se o DataFrame ainda possui estruturas aninhadas."""
        for field in df.schema.fields:
            if isinstance(field.dataType, (StructType, ArrayType, MapType)):
                return True
            if isinstance(field.dataType, ArrayType):
                if isinstance(field.dataType.elementType, (StructType, MapType)):
                    return True
        return False
    
    def _flatten_all_structs(self, df: DataFrame) -> DataFrame:
        """Achata todos os campos do tipo StructType de forma recursiva."""
        current_df = df
        
        while True:
            struct_fields = [field.name for field in current_df.schema.fields 
                           if isinstance(field.dataType, StructType)]
            
            if not struct_fields:
                break
            
            select_columns = []
            
            for field in current_df.schema.fields:
                if isinstance(field.dataType, StructType):
                    for sub_field in field.dataType.fields:
                        new_col_name = f"{field.name}{self.separator}{sub_field.name}"
                        select_columns.append(
                            col(f"{field.name}.{sub_field.name}").alias(new_col_name)
                        )
                else:
                    select_columns.append(col(field.name))
            
            current_df = current_df.select(*select_columns)
        
        return current_df
    
    def _flatten_all_arrays(self, df: DataFrame) -> DataFrame:
        """Achata todos os campos do tipo ArrayType de forma recursiva."""
        current_df = df
        
        while True:
            array_fields = [field.name for field in current_df.schema.fields 
                          if isinstance(field.dataType, ArrayType)]
            
            if not array_fields:
                break
            
            for array_field in array_fields:
                current_df = current_df.select(
                    "*",
                    posexplode_outer(col(array_field)).alias(f"{array_field}_pos", f"{array_field}_value")
                ).drop(array_field)
                
                current_df = current_df.withColumnRenamed(f"{array_field}_value", array_field)
                current_df = current_df.drop(f"{array_field}_pos")
                
                break
        
        return current_df
    
    def _flatten_all_maps(self, df: DataFrame) -> DataFrame:
        """Achata todos os campos do tipo MapType de forma recursiva."""
        current_df = df
        
        while True:
            map_fields = [field.name for field in current_df.schema.fields 
                        if isinstance(field.dataType, MapType)]
            
            if not map_fields:
                break
            
            for map_field in map_fields:
                current_df = current_df.select(
                    "*",
                    explode_outer(col(map_field)).alias(f"{map_field}_key", f"{map_field}_value")
                ).drop(map_field)
                
                current_df = current_df.withColumn(
                    map_field,
                    concat_ws("=", col(f"{map_field}_key"), col(f"{map_field}_value"))
                ).drop(f"{map_field}_key", f"{map_field}_value")
                
                break
        
        return current_df
    
    def _clean_column_names(self, df: DataFrame) -> DataFrame:
        """Limpa nomes de colunas removendo caracteres especiais."""
        cleaned_columns = []
        
        for col_name in df.columns:
            cleaned_name = (col_name.replace(' ', '_')
                                   .replace('-', '_')
                                   .replace('.', '_')
                                   .replace('(', '_')
                                   .replace(')', '_')
                                   .replace('[', '_')
                                   .replace(']', '_'))
            
            while '__' in cleaned_name:
                cleaned_name = cleaned_name.replace('__', '_')
            
            cleaned_name = cleaned_name.strip('_')
            
            cleaned_columns.append(col(col_name).alias(cleaned_name))
        
        return df.select(*cleaned_columns)


