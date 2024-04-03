import polars as pl
import requests
from bs4 import BeautifulSoup
import os
import concurrent.futures
import zipfile

#Column names
new_columns = [
    'cnpj_basic',
    'cnpj_order',
    'cnpj_dv',
    'identificador_matriz',
    'nome_fantasia',
    'situacao_cadastral',
    'data_situacao_cadastral',
    'motivo_situacao_cadastral',
    'nome_cidade_exterior',
    'pais',
    'data_inicio_atividade',
    'cnae_principal',
    'cnae_secundario',
    'tipo_logradouro',
    'logradouro',
    'numero',
    'complemento',
    'bairro',
    'cep',
    'uf',
    'municipio',
    'ddd_1',
    'telefone_1',
    'ddd_2',
    'telefone_2',
    'ddd_fax',
    'fax',
    'email',
    'situacao_especial',
    'data_situacao_especial'
]

#Data types
dtypes = {
    'cnpj_basic':pl.String,
    'cnpj_order':pl.String,
    'cnpj_dv':pl.String,
    'data_situacao_cadastral':pl.String,
    'identificador_matriz':pl.Int8,
    'situacao_cadastral':pl.Int8,
    'data_inicio_atividade':pl.String,
    'motivo_situacao_cadastral': pl.Int16,
    'telefone_1':pl.String,
    'telefone_2':pl.String,
    'fax':pl.String,
    'ddd_1':pl.String,
    'ddd_2':pl.String,
    'ddd_fax':pl.String,
    'pais': pl.Int32,
    'cnae_principal': pl.String,
    'cep': pl.String,
    'uf': pl.Categorical,
    'municipio': pl.Int32,
    'situacao_especial': pl.Categorical
}

#Get folders in extract directory (list)
extract_dir = os.listdir('data/raw/extract/')
empresa_part = []

# Get file names
for i in range(0,len(extract_dir)):
    file_part = os.listdir(f'data/raw/extract/{extract_dir[i]}/')[0]
    empresa_part.append(file_part)

print("Starting data reading")
for i in range(0,len(extract_dir)):
    print(f'Reading part: {extract_dir[i]}')
    df_emp = pl.read_csv(f'data/raw/extract/{extract_dir[i]}/{empresa_part[i]}',
                         encoding = 'latin1',
                         separator = ';',
                         has_header=False,
                         new_columns = new_columns,
                         dtypes=dtypes)
    

    print("Changing data types for dates")
    #Change data types for date
    df_emp = df_emp.with_columns(
        pl.col('data_situacao_cadastral').str.strptime(pl.Date,'%Y %m %d',strict=False).cast(pl.Date),
        pl.col('data_inicio_atividade').str.strptime(pl.Date,'%Y %m %d',strict=False).cast(pl.Date),
        pl.col('data_situacao_especial').str.strptime(pl.Date,'%Y %m %d',strict=False).cast(pl.Date)
    )

    print("Concating data")
    #Concating cnpj to a single column and logradouro / change cnae secundario type
    df_emp = df_emp.with_columns(
        pl.concat_str(
            [
                pl.col("cnpj_basic"),
                pl.col("cnpj_order"),
                pl.col("cnpj_dv"),
            ]
        ).alias("cnpj_completo"),
        pl.concat_str(
            [
                pl.col("tipo_logradouro"),
                pl.col("logradouro")
            ],
            separator = " ",
        ).alias("logradouro_completo"),
        pl.col('cnae_secundario').cast(pl.List(pl.Utf8))
    )

    # remove unused columns
    df_emp = df_emp.drop(['cnpj_basic','cnpj_order','cnpj_dv','tipo_logradouro','logradouro'])

    #Reordering df
    df_emp = df_emp.select(['cnpj_completo'] + [col for col in df_emp.columns if col not in ['cnpj_completo']])

    print("Dumping to parquet")
    #Dumping to parquet
    df_emp.write_parquet(f"C:/Users/Jonas/Documents/github/empresa-etl/data/domain/{extract_dir[i]}_silver.parquet")
    #Deleting frame to free memory space
    del df_emp
