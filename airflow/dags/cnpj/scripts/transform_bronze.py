def transform_bronze():
    from utils.util import _make_folder
    from utils.util import get_json_file
    from utils.util import set_dtypes
    from airflow.models import Variable
    import logging
    import os
    import polars as pl

    data_json = get_json_file('bronze_data.json')

    # Column names
    new_columns = data_json["bronze"]["columns"]

    # Data types
    dtypes = set_dtypes(data_json["bronze"]["dtypes"])

    # Getting variable from airflow UI
    DOMAIN_PATH = Variable.get('domain_path_var')

    # Get folders in extract directory (list)
    extract_dir = os.listdir("airflow/data/bronze/extract/")

    empresa_part = [ os.listdir(f"airflow/data/bronze/extract/{extract_dir[i]}/")[0] for i in range(0, len(extract_dir))]

    logging.info("Starting data reading")
    for i in range(0, len(extract_dir)):
        logging.info(f"Reading part: {extract_dir[i]}")
        df_emp = pl.read_csv(
            f"airflow/data/bronze/extract/{extract_dir[i]}/{empresa_part[i]}",
            encoding="latin1",
            separator=";",
            has_header=False,
            new_columns=new_columns,
            dtypes=dtypes,
        )

        print("Changing data types for dates")
        # Change data types for date
        df_emp = df_emp.with_columns(
            pl.col("data_situacao_cadastral")
            .str.strptime(pl.Date, "%Y %m %d", strict=False)
            .cast(pl.Date),
            pl.col("data_inicio_atividade")
            .str.strptime(pl.Date, "%Y %m %d", strict=False)
            .cast(pl.Date),
            pl.col("data_situacao_especial")
            .str.strptime(pl.Date, "%Y %m %d", strict=False)
            .cast(pl.Date),
        )

        print("Concating data")
        # Concating cnpj to a single column and logradouro / change cnae secundario type
        df_emp = df_emp.with_columns(
            pl.concat_str(
                [
                    pl.col("cnpj_basic"),
                    pl.col("cnpj_order"),
                    pl.col("cnpj_dv"),
                ]
            ).alias("cnpj_completo"),
            pl.concat_str(
                [pl.col("tipo_logradouro"), pl.col("logradouro")],
                separator=" ",
            ).alias("logradouro_completo"),
            pl.col("cnae_secundario").cast(pl.List(pl.Utf8)),
            pl.col(
                [
                    "nome_fantasia",
                    "bairro",
                    "complemento",
                    "cep",
                    "email",
                    "ddd_1",
                    "ddd_2",
                    "telefone_1",
                    "telefone_2",
                ]
            ).replace("", None),
        )

        # remove unused columns
        df_emp = df_emp.drop(
            ["cnpj_basic", "cnpj_order", "cnpj_dv", "tipo_logradouro", "logradouro"]
        )

        # Reordering df
        df_emp = df_emp.select(
            ["cnpj_completo"]
            + [col for col in df_emp.columns if col not in ["cnpj_completo"]]
        )

        _make_folder(DOMAIN_PATH)

        logging.info("Dumping to parquet")
        # Dumping to parquet
        df_emp.write_parquet(f"airflow/data/domain/{extract_dir[i]}_silver.parquet")
        # Deleting frame to free memory space
        del df_emp
