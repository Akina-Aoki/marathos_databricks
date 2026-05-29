from pyspark.sql.functions import col, date_format


DATE_ID_FORMAT = "yyyyMMdd"


def date_id_from(column_name, alias="date_id"):
    return date_format(col(column_name), DATE_ID_FORMAT).cast("int").alias(alias)