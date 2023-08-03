

import argparse
import os
from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit
from pyspark.sql.types import StringType
from builtins import str


from common.utils import Utils

# STEP 1 : INITIALIZING SPARK SESSION
# spark = SparkSession.builder.master("local[*]").appName("Raw Layer Injection").getOrCreate()

# spark = SparkSession.builder \
#         .master("local[*]") \
#         .appName("Raw Layer Ingestion") \
#         .getOrCreate()










class ETLProcessor:
    def __init__(self, dataset_name, run_date, base_path, file_extension, schema_path):
        self.d_name = dataset_name
        self.r_date = run_date
        self.b_path = base_path
        self.f_exe = file_extension
        self.s_path = schema_path

    def spark_initialization(self):
        self.spark = SparkSession.builder\
            .master("local[*]")\
            .appName("Raw Layer Injection")\
            .getOrCreate()

    def gen_run_date(self):
        if type(self.r_date) == str:
            return str(self.r_date).replace('-','')
        else:
            return datetime.strftime(self.r_date, "%Y%m%d")

    def path_validation(self):
        r_date_str = self.gen_run_date()

        input_dir = f"{self.b_path}/{self.d_name}/{r_date_str}"
        if not os.path.isdir(input_dir):
            raise Exception(f"{input_dir} does not exist.")

        input_file = f"{input_dir}/{self.d_name}.{self.f_exe}"
        if not os.path.exists(input_file):
            raise Exception(f"{input_file} does not exist.")


        schema_file = f"{self.s_path}\\{self.d_name}.json"
        if not os.path.exists(f"{schema_file}"):
            raise Exception(f"{schema_file} does not exist.")

        return input_file, schema_file

    def perform_plane_etl(self):
        self.spark_initialization()

        input_file, schema_file = self.path_validation()

        global null_check, datatype_check
        util = Utils()

        header, inferschema, column_lst = util.retrieve_schema(schema_file)

        input_schema = None
        if column_lst:
            input_schema = util.generate_custom_schema(column_lst)

        df = util.create_dataframe(spark=self.spark,
                                   file_format=self.f_exe,
                                   input_schema=input_schema,
                                   header=header,
                                   inferschema=inferschema,
                                   input_file=input_file)

        metadata_path = f"{self.s_path}/metadata.json"

        input_metadata = util.retrieve_json_obj(metadata_path)

        for val in input_metadata:

            metadata = val.get(self.d_name)
            null_check = metadata.get('nullcheck')
            datatype_check = metadata.get('datatypecheck')
            spe_char_check = metadata.get('special_character')
            value_map = metadata.get('value_mapping')



        df = df.withColumn("validation", lit(True))

        # Null check
        if null_check:
            df = df.withColumn("null_check_final", lit(True))
            for colm in null_check:
                df = df.withColumn("null_check",
                                   when(col(colm).isNull(), False).otherwise(True))
                df = df.withColumn("null_check_final",
                                   when(col("null_check") == False, False).otherwise(col("null_check_final")))

                # Update the "validation" column based on the null check
                df = df.withColumn("validation", when(col("null_check") == False, False).otherwise(col("validation")))

        # Datatype check
        if datatype_check:
            df = df.withColumn("datatype_check_final", lit(True))
            for colm in datatype_check:
                df = df.withColumn("datatype_check",
                                   when(col(colm).cast(StringType()).isNotNull(), True).otherwise(False))
                df = df.withColumn("datatype_check_final",
                                   when(col("datatype_check") == False, False).otherwise(col("datatype_check_final")))

                df = df.withColumn("validation",
                                   when(col("datatype_check") == False, False).otherwise(col("validation")))

        # special_char_check
        if spe_char_check:
            df = df.withColumn("spe_char_check_final", lit(True))
            for colm in spe_char_check:
                df = df.withColumn("spe_char_check",
                                   when(col(colm).rlike('[!@$%^&*(){}]'), False).otherwise(True))

                df = df.withColumn("spe_char_check_final",
                                   when(col("spe_char_check") == False, False).otherwise(col("spe_char_check_final")))

                df = df.withColumn("validation",
                                   when(col("spe_char_check") == False, False).otherwise(col("validation")))

        columns_to_drop = ["spe_char_check", "datatype_check", "null_check"]

        # Drop the specified columns
        df = df.drop(*columns_to_drop)



        # STEP : FINAL ---READING THE DATA----

        # good_data = df.select(df.name, df.iata_code, df.icao_code, df.validation).where(col("validation") == True)
        #
        # bad_data = df.select(df.name, df.iata_code, df.icao_code, df.validation).where(col("validation") == False)
        #
        # good_data.show()
        # bad_data.show()

        # good_data.write.parquet(r"D:\02_CS\Big_Data\ALL_PROJECT\_1_airline\output\good")
        # bad_data.write.parquet(r"D:\02_CS\Big_Data\ALL_PROJECT\_1_airline\output\bad")

        # df1 = self.spark.read.parquet(r"D:\02_CS\Big_Data\ALL_PROJECT\_1_airline\output\good")
        # df2 = self.spark.read.parquet(r"D:\02_CS\Big_Data\ALL_PROJECT\_1_airline\output\bad")
        #
        # df1.show()
        # df2.show()
        #




        # value_mapping
        if value_map:
            for col_nm,map in value_map.items():
                for letter,country in map.items():
                    df = df.withColumn(col_nm, when(col(col_nm)==letter, country).otherwise(col(col_nm)))





        df.show(100)



if __name__ == "__main__":

    # STEP 2 : CREATING ARGPARSE OBJECT
    parser = argparse.ArgumentParser()

    parser.add_argument("--dataset_name",
                        help="Provide Dataset name")
    parser.add_argument("--run_date",
                        default=date.today(),
                        help="Provide run date ")
    parser.add_argument("--base_path",
                        default="D:\_02_CS\Big_Data\ALL_PROJECT\_2_airport\project\stagging",
                        help="Provide base path")
    parser.add_argument("--file_extension",
                        default="csv",
                        help="Provide correct input/staging file extension")
    parser.add_argument("--schema_path",
                        default="D:\_02_CS\Big_Data\ALL_PROJECT\_2_airport\schema",
                        help="Provide schema path")

    args = parser.parse_args()

    etl_processor = ETLProcessor(args.dataset_name, args.run_date, args.base_path, args.file_extension,
                                 args.schema_path)
    etl_processor.perform_plane_etl()














