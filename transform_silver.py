import polars as pl
import os

# Set column names for support dataframes

motivo_column = ['situacao_cadastral','descricao_motivo']
municipio_column = ['municipio','descricao_municipio']
pais_column = ['pais','descricao_pais']

# Set support dataframes data types

dtypes_motivo = {'situacao_cadastral':pl.Int8,'descricao_motivo':pl.Categorical}
dtypes_municipio = {'municipio':pl.Int32}
dtypes_pais = {'pais':pl.Int32}

# Setting support data frames paths and reading

#Motivo
support_motivo_path = os.listdir('data/raw/support/motivos/')[0]
df_mot = pl.read_csv(f'data/raw/support/motivos/{support_motivo_path}', separator = ';', new_columns = motivo_column,dtypes=dtypes_motivo)

#Municipio
support_municipio_path = os.listdir('data/raw/support/municipios/')[0]
df_mun = pl.read_csv(f'data/raw/support/municipios/{support_municipio_path}',separator = ';',new_columns = municipio_column, dtypes=dtypes_municipio)

#Pais
support_pais_path = os.listdir('data/raw/support/paises/')[0]
df_paises = pl.read_csv(f'data/raw/support/paises/{support_pais_path}',separator = ';',encoding='latin1', new_columns = pais_column, dtypes=dtypes_pais)

#Directory for dataframe files
estabelecimentos_list = os.listdir('data/domain')

select_columns = [
    'cnpj_completo',
    'nome_fantasia',
    'identificador_matriz',
    'data_inicio_atividade',
    'descricao_pais',
    'uf',
    'descricao_municipio',
    'cep',
    'logradouro_completo',
    'bairro',
    'numero',
    'complemento',
    'ddd1_alias',
    'telefone_1',
    'ddd2_alias',
    'telefone_2',
    'email',
    'cnae_principal',
    'descricao_motivo'
]

i=0
for file in estabelecimentos_list:
    if file.endswith('.parquet'):
        print(f'reading file: {file}')
        df_emp = pl.read_parquet(f'data/domain/{file}')

        # Data cleaning for ddd's columns / change for company type
        print('Cleaning data')
        df_emp = df_emp.with_columns(
            pl.when(~pl.col('ddd_1').str.contains("-?\d+(?:\.\d+)?")).then(pl.lit(None)).otherwise(pl.col('ddd_1')).alias('ddd1_alias').str.strip_chars().cast(pl.Int32),
            pl.when(~pl.col('ddd_2').str.contains("-?\d+(?:\.\d+)?")).then(pl.lit(None)).otherwise(pl.col('ddd_2')).alias('ddd2_alias').str.strip_chars().cast(pl.Int32),
            pl.col('identificador_matriz').replace([1,2],['matriz','filial']).cast(pl.Categorical)
        ).drop(['ddd_1','ddd_2'])

        print('Joining data')

        # Joining data
        #Sit. cadastral
        df_emp = df_emp.join(df_mot, on="situacao_cadastral",how='left')

        #Municipio
        df_emp = df_emp.join(df_mun, on="municipio",how='left')

        #Paises
        df_emp = df_emp.join(df_paises, on="pais",how='left')

        print(f'Dumping matriz,file: {i}')
        #Dump to parquet for matriz
        df_emp.select(select_columns).filter(pl.col('identificador_matriz')=='matriz').write_parquet(f'data/domain/silver_augmented/matriz/estabelecimento_matriz_{i}.parquet')

        print(f'Dumping filial,file: {i}')
        #Dump to parquet for filial
        df_emp.select(select_columns).filter(pl.col('identificador_matriz')=='filial').write_parquet(f'data/domain/silver_augmented/filial/estabelecimento_filial_{i}.parquet')

        i = i+1

        del df_emp
