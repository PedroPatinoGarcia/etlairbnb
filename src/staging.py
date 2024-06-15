from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, current_date
from pyspark.sql.types import ArrayType
from datetime import datetime
import os

spark = SparkSession.builder.appName("BusinessAirBnB").getOrCreate()

class HandlerBranchBusiness:
    @staticmethod
    def get_latest_parquet_files(directory):
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
        # Obtener las columnas del DataFrame
        columns = df.columns

        # Iterar sobre las columnas y transformar arrays de tipo string
        for col_name in columns:
            col_type = df.schema[col_name].dataType
            if isinstance(col_type, ArrayType) and col_type.elementType.typeName() == "string":
                df = df.withColumn(col_name, concat_ws(", ", col(col_name)))

        df = df.withColumn('processed_date', current_date())
        return df

    @staticmethod
    def partition_folder(base_path):
        current_date = datetime.now()
        year = current_date.year
        month = current_date.month
        day = current_date.day

        path = os.path.join(base_path, str(year), str(month), str(day))
        os.makedirs(path, exist_ok=True)
        return path

    @staticmethod
    def export_to_csv(df, output_path):
        try:
            temp_output_path = os.path.join(output_path, "temp_csv_output")
            os.makedirs(temp_output_path, exist_ok=True)

            timestamp_suffix = datetime.now().strftime('%Y%m%d%H%M%S')
            final_csv_path = os.path.join(output_path, f"data-{timestamp_suffix}.csv")
            
            # Guardar el CSV con un nombre único
            df.coalesce(1).write.mode("overwrite").csv(temp_output_path, header=True)

            temp_file = [f for f in os.listdir(temp_output_path) if f.endswith('.csv')][0]
            os.rename(os.path.join(temp_output_path, temp_file), final_csv_path)

            print(f"Datos exportados a CSV en {final_csv_path}")
        except Exception as e:
            print(f"Error al exportar a CSV: {e}")

    @staticmethod
    def process_latest_staging():
        staging_path = HandlerBranchBusiness.partition_folder('staging')
        latest_files = HandlerBranchBusiness.get_latest_parquet_files(staging_path)

        for latest_file in latest_files:
            try:
                print(f"Procesando archivo: {latest_file}")
                df = spark.read.parquet(latest_file)
                business_df = HandlerBranchBusiness.process_data(df)
                
                if business_df.count() > 0:
                    business_path = HandlerBranchBusiness.partition_folder('business')
                    HandlerBranchBusiness.export_to_csv(business_df, business_path)
                    output_path = os.path.join(business_path, f"data-{datetime.now().strftime('%Y-%m-%d-%H%M%S')}.parquet")  # Nombre único con timestamp
                    business_df.coalesce(1).write.mode("overwrite").parquet(output_path)
                    print(f"Datos procesados y guardados en {output_path}")
                else:
                    print("El DataFrame está vacío después del procesamiento. No se guardó ningún archivo.")
            except Exception as e:
                print(f"Error al procesar archivo {latest_file}: {e}")
        
        if not latest_files:
            print("No se encontraron archivos de datos de staging para procesar.")

if __name__ == "__main__":
    HandlerBranchBusiness.process_latest_staging()
