import polars as pl
import os
from scripts.download_data import _make_folder

def transform_silver():

    # Set column names for support dataframes

    motivo_column = ["situacao_cadastral", "descricao_motivo"]
    municipio_column = ["municipio", "descricao_municipio"]
    pais_column = ["pais", "descricao_pais"]

    # Set support dataframes data types

    dtypes_motivo = {"situacao_cadastral": pl.Int8, "descricao_motivo": pl.Categorical}
    dtypes_municipio = {"municipio": pl.Int32}
    dtypes_pais = {"pais": pl.Int32}

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

    select_columns = [
        "cnpj_completo",
        "nome_fantasia",
        "identificador_matriz",
        "data_inicio_atividade",
        "descricao_pais",
        "uf",
        "descricao_municipio",
        "cep",
        "logradouro_completo",
        "bairro",
        "numero",
        "complemento",
        "ddd1_alias",
        "telefone_1",
        "ddd2_alias",
        "telefone_2",
        "email",
        "cnae_principal",
        "descricao_motivo",
    ]


    for index, file in enumerate(estabelecimentos_list):
        if file.endswith(".parquet"):
            print(f"reading file: {file}")
            df_emp = pl.read_parquet(f"airflow/data/domain/{file}")

            # Data cleaning for ddd's columns / change for company type
            print("Cleaning data")
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

            print(f"Dumping matriz,file: {index}")
            # Dump to parquet for matriz
            df_emp.select(select_columns).filter(
                pl.col("identificador_matriz") == "matriz"
            ).write_parquet(f"airflow/data/gold/matriz/estabelecimento_matriz_{index}.parquet")

            print(f"Dumping filial,file: {index}")
            # Dump to parquet for filial
            df_emp.select(select_columns).filter(
                pl.col("identificador_matriz") == "filial"
            ).write_parquet(f"airflow/data/gold/filial/estabelecimento_filial_{index}.parquet")

            del df_emp
