import os
from datetime import datetime
import time
from timeit import default_timer
from spark_session import spark

os.makedirs('raw', exist_ok=True)
os.makedirs('staging', exist_ok=True)
os.makedirs('business', exist_ok=True)

class HandlerBranchCode:
    @staticmethod
    def read_csv(file_path):
        """
        Lee un archivo CSV utilizando Spark DataFrame.

        :param file_path: Ruta al archivo CSV.
        :return: DataFrame de Spark si la lectura es exitosa, None en caso de error.
        """
        try:
            df = spark.read.csv(file_path, header=True, inferSchema=True, escape='"', quote='"')
            return df
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return None

    @staticmethod
    def partition_folder(base_path):
        """
        Crea una estructura de carpetas basada en la fecha actual dentro del directorio especificado.

        :param base_path: Ruta base donde se creará la estructura de carpetas.
        :return: Ruta completa de la carpeta creada.
        """
        current_date = datetime.now()
        year = current_date.year
        month = current_date.month
        day = current_date.day

        path = os.path.join(base_path, str(year), str(month), str(day))
        os.makedirs(path, exist_ok=True)
        return path

    @staticmethod
    def write_data(df, output_path):
        """
        Escribe el DataFrame dado en formato Parquet en la ruta especificada.

        :param df: DataFrame de Spark a escribir.
        :param output_path: Ruta donde se almacenará el archivo Parquet.
        """
        if df:
            start = default_timer()
            df_single_partition = df.coalesce(1)
            df_single_partition.write.mode("overwrite").parquet(output_path)

            end = default_timer() - start
            time.sleep(max(0, 30 - end))
        else:
            print("No data to write.")

def main():
    csv_file_path = './Airbnb_Data.csv'
    df = HandlerBranchCode.read_csv(csv_file_path)
    if df is not None:
        raw_path = HandlerBranchCode.partition_folder('raw')
        HandlerBranchCode.write_data(df, raw_path)

        file_paths = [os.path.join(raw_path, f) for f in os.listdir(raw_path) if f.endswith('.parquet')]
        print(file_paths)
    else:
        print("No CSV data was loaded, no further processing.")

if __name__ == "__main__":
    main()
