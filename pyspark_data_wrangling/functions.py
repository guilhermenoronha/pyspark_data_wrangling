"""Module with useful data wrangling methods 

Methods:
    unnest_struct_columns: method to unnest all struct columns from a dataframe.
"""

import pyspark.sql.functions as f
from pyspark.sql import DataFrame


def unnest_struct_columns(df: DataFrame) -> DataFrame:
    """Unnest all struct columns from a dataframe.
    PS: this method only unnest the struct columns in the first level of nesting.

    Args:
        df (DataFrame): a spark dataframe with at least one struct column

    Returns:
        DataFrame: a spark dataframe with no struct columns
    """
    # List to hold the dynamically generated column names
    flattened_col_list = []

    # Inner method to iterate over Data Frame to generate the column list
    def _get_flattened_cols(df: DataFrame, struct_col: str = None) -> None:
        for col in df.columns:
            if df.schema[col].dataType.typeName() != "struct":
                if struct_col is None:
                    flattened_col_list.append(f"{col} as {col.replace('.','_')}")
                else:
                    t = struct_col + "." + col
                    flattened_col_list.append(f"{t} as {t.replace('.','_')}")
            else:
                chained_col = struct_col + "." + col if struct_col is not None else col
                _get_flattened_cols(df.select(col + ".*"), chained_col)

    # Call the inner Method
    _get_flattened_cols(df)

    # Return the flattened Data Frame
    return df.selectExpr(flattened_col_list)


def explode_array_columns(df: DataFrame) -> DataFrame:
    """Explode every array column of a dataframe.
    PS: this method only explode the array columns in the first level of columns.

    Args:
        df (DataFrame): a spark dataframe with at least one array column

    Returns:
        DataFrame: a spark dataframe with no array columns
    """
    for col in df.columns:
        if df.schema[col].dataType.typeName() == "array":
            df = df.withColumn(col, f.explode_outer(col))
    return df


def flatten_json_df(df: DataFrame) -> DataFrame:
    """Removes every array and struct columns from the dataframe.

    Args:
        df (DataFrame): a spark dataframe with at least one array or struct column

    Returns:
        DataFrame: a spark dataframe with no array  nor struct columns
    """
    while any(
        df.schema[col].dataType.typeName() in ["array", "struct"] for col in df.columns
    ):
        df = explode_array_columns(df)
        df = unnest_struct_columns(df)
    return df
