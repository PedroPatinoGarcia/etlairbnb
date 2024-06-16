from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col
from pyspark.sql.types import ArrayType
from datetime import datetime
import os
import shutil

spark = SparkSession.builder.appName("BusinessAirBnB").getOrCreate()

class HandlerBranchBusiness:
    @staticmethod
    def get_latest_parquet_files(directory):
        """
        Obtiene los archivos más recientes en formato Parquet desde un directorio especificado.

        Args:
            directory (str): Directorio donde buscar archivos Parquet.

        Returns:
            list: Lista de rutas completas a los archivos Parquet encontrados.
        """
        try:
            files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.parquet')]
            if not files:
                raise ValueError(f"No Parquet files found in {directory}.")
            return files
        except Exception as e:
            print(f"Error getting the latest Parquet files: {e}")
            return []

    @staticmethod
    def process_data(df):
        """
        Procesa un DataFrame Spark, convirtiendo los arrays de tipo string en strings separados por comas.

        Args:
            df (DataFrame): DataFrame Spark a procesar.

        Returns:
            DataFrame: DataFrame procesado.
        """

        for col_name in df.columns:
            col_type = df.schema[col_name].dataType
            if isinstance(col_type, ArrayType) and col_type.elementType.typeName() == "string":
                df = df.withColumn(col_name, concat_ws(", ", col(col_name)))

        return df

    @staticmethod
    def partition_folder(base_path):
        """
        Crea una estructura de carpetas basada en la fecha actual dentro de un directorio base.

        Args:
            base_path (str): Directorio base donde crear la estructura de carpetas.

        Returns:
            str: Ruta completa de la carpeta creada.
        """
        current_date = datetime.now()
        year = current_date.year
        month = current_date.month
        day = current_date.day

        path = os.path.join(base_path, str(year), str(month), str(day))
        os.makedirs(path, exist_ok=True)
        return path

    @staticmethod
    def export_to_csv(df, output_path, csv_name):
        """
        Exporta un DataFrame Spark a un archivo CSV en una ruta especificada, con nombre fijo basado en la fecha.

        Args:
            df (DataFrame): DataFrame Spark a exportar.
            output_path (str): Ruta donde guardar el archivo CSV.
            csv_name (str): Nombre base del archivo CSV.

        """
        try:
            temp_output_path = os.path.join(output_path, "temp_csv_output")
            if os.path.exists(temp_output_path):
                shutil.rmtree(temp_output_path)
            
            os.makedirs(temp_output_path, exist_ok=True)

            final_csv_path = os.path.join(output_path, f"{csv_name}-{datetime.now().strftime('%Y-%m-%d')}.csv")
            df.coalesce(1).write.mode("overwrite").csv(temp_output_path, header=True)
            temp_file = [f for f in os.listdir(temp_output_path) if f.endswith('.csv')][0]
            temp_file_path = os.path.join(temp_output_path, temp_file)

            os.rename(temp_file_path, final_csv_path)

            print(f"Datos exportados a CSV en {final_csv_path}")

            shutil.rmtree(temp_output_path)
        except Exception as e:
            print(f"Error al exportar a CSV: {e}")

    @staticmethod
    def process_latest_staging():
        """
        Procesa el archivo Parquet más reciente en el directorio 'staging'.
        Procesa los datos y exporta los resultados a CSV en una carpeta 'business'.
        """
        staging_path = HandlerBranchBusiness.partition_folder('staging')
        latest_files = HandlerBranchBusiness.get_latest_parquet_files(staging_path)

        if latest_files:
            latest_file = latest_files[0]
            
            try:
                print(f"Procesando archivo: {latest_file}")
                df = spark.read.parquet(latest_file)
                
                df_name = "data"

                business_df = HandlerBranchBusiness.process_data(df)
                
                if business_df.count() > 0:
                    output_path = os.path.join('business', f"{df_name}-{datetime.now().strftime('%Y-%m-%d')}")
                    HandlerBranchBusiness.export_to_csv(business_df, output_path, df_name)
                    print(f"Datos procesados y guardados en {output_path}.csv")
                else:
                    print("El DataFrame está vacío después del procesamiento. No se guardó ningún archivo.")
            
            except Exception as e:
                print(f"Error al procesar archivo {latest_file}: {e}")
        
        else:
            print("No se encontraron archivos de datos de staging para procesar.")

    if __name__ == "__main__":
        process_latest_staging()

