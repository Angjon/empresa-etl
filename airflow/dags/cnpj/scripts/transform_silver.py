def transform_silver():
    import polars as pl
    import os
    import logging
    from airflow.models import Variable
    from utils.util import _make_folder
    from utils.util import get_json_file
    from utils.util import set_dtypes

    data_json = get_json_file('silver_data.json')

    # Set column names for support dataframes
    motivo_column = data_json['silver']['motivo_column']
    municipio_column = data_json['silver']['municipio_column']
    pais_column = data_json['silver']['pais_column']

    # Set support dataframes data types
    dtypes_motivo = set_dtypes(data_json['silver']['dtypes_motivo'])
    dtypes_municipio = set_dtypes(data_json['silver']['dtypes_municipio'])
    dtypes_pais = set_dtypes(data_json['silver']['dtypes_pais'])

    # Setting support data frames paths and reading
    # Motivo
    support_motivo_path = os.listdir("airflow/data/bronze/support/motivos/")[0]
    df_mot = pl.read_csv(
        f"airflow/data/bronze/support/motivos/{support_motivo_path}",
        separator=";",
        new_columns=motivo_column,
        dtypes=dtypes_motivo,
    )

    # Municipio
    support_municipio_path = os.listdir("airflow/data/bronze/support/municipios/")[0]
    df_mun = pl.read_csv(
        f"airflow/data/bronze/support/municipios/{support_municipio_path}",
        separator=";",
        new_columns=municipio_column,
        dtypes=dtypes_municipio,
    )

    # Pais
    support_pais_path = os.listdir("airflow/data/bronze/support/paises/")[0]
    df_paises = pl.read_csv(
        f"airflow/data/bronze/support/paises/{support_pais_path}",
        separator=";",
        encoding="latin1",
        new_columns=pais_column,
        dtypes=dtypes_pais,
    )

    # Directory for dataframe files
    estabelecimentos_list = os.listdir("airflow/data/domain")

    select_columns = data_json['silver']['select_columns']


    for index, file in enumerate(estabelecimentos_list):
        if file.endswith(".parquet"):
            logging.info(f"Reading file: {file}")
            df_emp = pl.read_parquet(f"airflow/data/domain/{file}")

            # Data cleaning for ddd's columns / change for company type
            logging.info("Cleaning data")
            df_emp = df_emp.with_columns(
                pl.when(~pl.col("ddd_1").str.contains("-?\d+(?:\.\d+)?"))
                .then(pl.lit(None))
                .otherwise(pl.col("ddd_1"))
                .alias("ddd1_alias")
                .str.strip_chars()
                .cast(pl.Int32),
                pl.when(~pl.col("ddd_2").str.contains("-?\d+(?:\.\d+)?"))
                .then(pl.lit(None))
                .otherwise(pl.col("ddd_2"))
                .alias("ddd2_alias")
                .str.strip_chars()
                .cast(pl.Int32),
                pl.col("identificador_matriz")
                .replace([1, 2], ["matriz", "filial"])
                .cast(pl.Categorical),
            ).drop(["ddd_1", "ddd_2"])

            print("Joining data")

            # Joining data
            # Sit. cadastral
            df_emp = df_emp.join(df_mot, on="situacao_cadastral", how="left")

            # Municipio
            df_emp = df_emp.join(df_mun, on="municipio", how="left")

            # Paises
            df_emp = df_emp.join(df_paises, on="pais", how="left")


            _make_folder('airflow/data/gold/matriz')
            _make_folder('airflow/data/gold/filial')

            logging.info(f"Dumping matriz,file: {index}")
            # Dump to parquet for matriz
            df_emp.select(select_columns).filter(
                pl.col("identificador_matriz") == "matriz"
            ).write_parquet(f"airflow/data/gold/matriz/estabelecimento_matriz_{index}.parquet")

            logging.info(f"Dumping filial,file: {index}")
            # Dump to parquet for filial
            df_emp.select(select_columns).filter(
                pl.col("identificador_matriz") == "filial"
            ).write_parquet(f"airflow/data/gold/filial/estabelecimento_filial_{index}.parquet")

            del df_emp
