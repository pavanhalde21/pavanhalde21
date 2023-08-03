import os, json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType


class Utils:
    def retrieve_json_obj(self, schema_path):
        with open(schema_path) as f:
            input_schema = json.loads(f.read())
        return input_schema

    def retrieve_schema(self, schema_path):
        # get data from schema file
        header, inferschema = "false", "false"

        input_schema = self.retrieve_json_obj(schema_path)

        for val in input_schema:
            for key, value in val.items():
                if key.lower() == 'header':
                    header = value
                elif key.lower() == 'inferschema':
                    inferschema = value
                elif key.lower() == 'column_list':
                    column_lst = value

        return header, inferschema, column_lst

    def generate_custom_schema(self, column_lst):
        # generate custom schema
        struct_field_lst = []

        for col in column_lst:
            # print(col, type(col))
            for key, value in col.items():
                if str(value).lower() == 'string':
                    struct_field_lst.append(StructField(key, StringType()))  # key becomes the column name and datatype of string can only be inserted
                elif str(value).lower() == 'integer':
                    struct_field_lst.append(StructField(key, IntegerType()))

                elif str(value).lower() == 'decimal':
                    struct_field_lst.append(StructField(key, DecimalType(18,9)))

        input_schema_struct = StructType(struct_field_lst)

        return input_schema_struct

    def create_dataframe(self, spark, file_format, input_schema, header, inferschema, input_file):
        if str(file_format).lower() == 'csv':
            if not input_schema: # checks if empty
                if header == 'false' and inferschema == 'false':
                    raise Exception("Please provide either header/inferschema as true or provide custom schema")

                else:
                    df = spark.read.format(file_format) \
                        .option("header", "true") \
                        .option("inferSchema", "true") \
                        .load(f"{input_file}")

            elif header == "true":
                df = spark.read.format(file_format) \
                    .schema(input_schema) \
                    .option("header", "true") \
                    .load(input_file)
            else:
                df = spark.read.format(file_format) \
                    .schema(input_schema) \
                    .load(input_file)

            return df
