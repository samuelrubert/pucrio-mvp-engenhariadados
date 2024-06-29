# Databricks notebook source
# MAGIC %md
# MAGIC # Objetivo: 
# MAGIC Extracao de dados relacionados a captura, utilização e armazenamento de carbono (CCUS).

# COMMAND ----------

# MAGIC %md 
# MAGIC # Configurações iniciais

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bibliotecas

# COMMAND ----------

!pip install openpyxl --quiet
!pip install unidecode --quiet

# COMMAND ----------

import pandas as pd
import re
from pyspark.sql import functions as sf
from unidecode import unidecode

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Parâmetros

# COMMAND ----------

# Definições do arquivo Excel
sheetname = "CCUS Projects Database"
cellposition = "A1"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Caminhos

# COMMAND ----------

# Caminho dos dados
path_arquivo = "https://github.com/samuelrubert/pucrio-mvp-engenhariadados/raw/ccus_mvp/Dados/IEA_CCUS_Projects_Database.xlsx"

# COMMAND ----------

# MAGIC %md
# MAGIC # Extração

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura do arquivo Excel

# COMMAND ----------

# Ler pandas pois o spark não consegue ler deste local
df_pandas = pd.read_excel(path_arquivo, sheet_name=sheetname, header=0)
df_pandas.head()

# COMMAND ----------

# Verificar os tipos de dados inferidos pelo pandas
df_pandas.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conversão em dataframe Spark

# COMMAND ----------

df = spark.createDataFrame(df_pandas)
display(df.limit(10))

# COMMAND ----------

# Este seria o bronze

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformação

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tratar nomes das colunas
# MAGIC As colunas devem ser renomeadas para serem compatíveis com uma tabela em banco de dados.

# COMMAND ----------

# Dicionário de nomes
dict_rename = {}

# Criar nomes tratados
for c in df.columns:
    nome_ok = c.strip()
    nome_ok = nome_ok.replace('/', '_')
    nome_ok = re.sub('[^A-Za-z0-9_ ]+', '', unidecode(nome_ok))
    nome_ok = re.sub('\s+','_', nome_ok)
    dict_rename[c] = nome_ok

# Renomear colunas no dataframe
df = df.withColumnsRenamed(dict_rename)

display(df.limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tratar dados

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remover NaN
# MAGIC Os valores NaN nas colunas do tipo float são substituídos por nulos, pois eles interferem nas agregações de valores em certas ferramentas, como o Power BI.
# MAGIC Ademais, vamos remover os valores NaN nas colunas de texto, pois se trata de um valor sem significado para o público de negócio.

# COMMAND ----------

df = df.replace(float("nan"), None)
df = df.replace("NaN", None)

display(df.limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Converter colunas em tipo inteiro
# MAGIC Quando fizemos a leitura do arquivo, algumas colunas foram inferidas como tipo float, quando na verdade tratam de valores inteiros.

# COMMAND ----------

colunas_cast_int = [
    "Announcement",
    "FID",
    "Operation",
    "Suspension_decommissioning",
    "Project_phase"
]
colunas_cast_int = {c: df[c].cast("int") for c in colunas_cast_int}

df = df.withColumns(colunas_cast_int)

display(df.limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Eliminar colunas Link
# MAGIC No arquivo Excel, as colunas Link possuem um hiperlink clicável para o endereço contido na coluna Ref de mesmo número. Esse recurso não se aplica a uma base de dados, por isso vamos remover as colunas Link e manter as colunas Ref.

# COMMAND ----------

colunas_drop = [c for c in df.columns if c.startswith("Link")]
df = df.drop(*colunas_drop)

display(df.limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Não iremos remover as colunas Ref totalmente em branco, porque um dia podem colocar dado e quebrar a tabela, etc etc

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tratar coluna Announced_capacity_Mt_CO2_yr
# MAGIC Esta coluna possui alguns valores expressos em faixas, como "0.3 - 0.8". Para permitir cálculos e agregações, vamos transformar essa coluna em duas: uma contendo o valor mínimo, outra contendo o valor máximo. Valores da coluna original que não são expressos em faixas serão repetidos, de forma a manter a integridade da informação de maneira independente nas duas colunas.

# COMMAND ----------

split_col = sf.split(df['Announced_capacity_Mt_CO2_yr'], "-")

# Criar coluna de valor mínimo
df = df.withColumn('Announced_capacity_low_Mt_CO2_yr', sf.when(sf.col('Announced_capacity_Mt_CO2_yr').contains('-'), split_col.getItem(0)).otherwise(sf.col('Announced_capacity_Mt_CO2_yr')).cast('double'))

# Criar coluna de valor máximo
df = df.withColumn('Announced_capacity_high_Mt_CO2_yr', sf.when(sf.col('Announced_capacity_Mt_CO2_yr').contains('-'), split_col.getItem(1)).otherwise(sf.col('Announced_capacity_Mt_CO2_yr')).cast('double'))

# Drop the original 'value' column
df = df.drop('Announced_capacity_Mt_CO2_yr')

columns_order = ['Project_name', 'ID', 'Country', 'Partners', 'Project_type', 'Announcement', 'FID', 'Operation', 'Suspension_decommissioning', 'Project_Status', 'Project_phase', 'Announced_capacity_low_Mt_CO2_yr', 'Announced_capacity_high_Mt_CO2_yr', 'Estimated_capacity_by_IEA_Mt_CO2_yr', 'Sector', 'Fate_of_carbon', 'Part_of_CCUS_hub', 'Region', 'Link_1', 'Link_2', 'Link_3', 'Link_4', 'Link_5', 'Link_6', 'Link_7']

df = df.select(*columns_order)


# COMMAND ----------

L = [1, 2, 3, 4, 5]

# Encontra a posição do elemento 3
posicao = L.index(3)

# Remove o elemento 3
L.remove(3)

# Insere os elementos 6 e 7 na posição encontrada
L.insert(posicao, 6)
L.insert(posicao + 1, 7)

print(L)  # [1, 2, 6, 7, 4, 5]

# COMMAND ----------

# MAGIC %md
# MAGIC # Carga

# COMMAND ----------

# Fazer comments!!!

# COMMAND ----------

# Notebook separado de análises
