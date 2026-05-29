import re


def to_snake_case(column_name):
    clean_name = column_name.strip().lower()
    clean_name = re.sub(r"[^a-z0-9]+", "_", clean_name)
    return clean_name.strip("_")


def rename_columns_to_snake_case(df):
    new_columns = [to_snake_case(column) for column in df.columns]
    return df.toDF(*new_columns)