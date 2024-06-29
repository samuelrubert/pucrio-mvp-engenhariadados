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
path_arquivo = "https://github.com/samuelrubert/pucrio-mvp-engenhariadados/raw/main/Dados/IEA_CCUS_Projects_Database.xlsx"

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
# MAGIC ### Tratar coluna Announced_capacity_Mt_CO2_yr
# MAGIC Essa coluna possui alguns valores expressos em faixas, como "0.3 - 0.8". Para permitir cálculos e agregações, vamos transformar essa coluna em duas: uma contendo o valor mínimo, outra contendo o valor máximo. Valores da coluna original que não sejam expressos em faixas serão repetidos, de forma a manter a integridade da informação de maneira independente nas duas colunas.

# COMMAND ----------

# Dividir a coluna
split_col = sf.split(df['Announced_capacity_Mt_CO2_yr'], "-")

# Criar coluna de valor mínimo
df = df.withColumn('Announced_capacity_low_Mt_CO2_yr', sf.when(sf.col('Announced_capacity_Mt_CO2_yr').contains('-'), split_col.getItem(0)).otherwise(sf.col('Announced_capacity_Mt_CO2_yr')).cast('double'))

# Criar coluna de valor máximo
df = df.withColumn('Announced_capacity_high_Mt_CO2_yr', sf.when(sf.col('Announced_capacity_Mt_CO2_yr').contains('-'), split_col.getItem(1)).otherwise(sf.col('Announced_capacity_Mt_CO2_yr')).cast('double'))

# Substituir coluna original pelas novas
colunas = df.columns
pos = colunas.index("Announced_capacity_Mt_CO2_yr")
colunas[pos] = "Announced_capacity_low_Mt_CO2_yr"
colunas.insert(pos+1, "Announced_capacity_high_Mt_CO2_yr")
colunas = colunas[:-2]
df = df.select(colunas)

display(df.limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC # Carga

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definir descrição da tabela e comentários dos campos

# COMMAND ----------

descricao = "A tabela contém informações sobre projetos de captura, utilização e armazenamento de carbono (CCUS) ao redor do mundo. Ela inclui dados como o nome dos projetos, país, parceiros envolvidos, tipo de projeto, ano de anúncio, ano da decisão final de investimento, ano de operação, ano de suspensão/descomissionamento, status do projeto, fase do projeto, capacidade anunciada e estimada para captura, transporte e armazenamento de CO2, setor, destino do carbono, se o projeto faz parte de um hub CCUS, região, e links para artigos de notícias sobre o projeto. Esta tabela fornece insights valiosos sobre o panorama global dos projetos de CCUS e seu status atual."

# COMMAND ----------

comentarios = {
    "Project_name": "Nome do projeto",
    "ID": "Identificação única do projeto",
    "Country": "País onde o projeto está localizado",
    "Partners": "Parceiros envolvidos no projeto",
    "Project_type": "Tipo de projeto",
    "Announcement": "Ano de anúncio do projeto",
    "FID": "Ano da decisão final de investimento",
    "Operation": "Ano de início da operação do projeto",
    "Suspension_decommissioning": "Ano de suspensão ou descomissionamento do projeto",
    "Project_Status": "Status atual do projeto",
    "Project_phase": "Fase atual do projeto",
    "Announced_capacity_low_Mt_CO2_yr": "Capacidade mínima anunciada para captura de CO2 em milhões de toneladas por ano",
    "Announced_capacity_high_Mt_CO2_yr": "Capacidade máxima anunciada para captura de CO2 em milhões de toneladas por ano",
    "Estimated_capacity_by_IEA_Mt_CO2_yr": "Capacidade estimada pela IEA para captura de CO2 em milhões de toneladas por ano",
    "Sector": "Setor industrial do projeto",
    "Fate_of_carbon": "Destino do carbono capturado",
    "Part_of_CCUS_hub": "Indica se o projeto faz parte de um hub CCUS",
    "Region": "Região onde o projeto está localizado",
    "Ref_1": "Link para o primeiro artigo de notícias sobre o projeto",
    "Ref_2": "Link para o segundo artigo de notícias sobre o projeto",
    "Ref_3": "Link para o terceiro artigo de notícias sobre o projeto",
    "Ref_4": "Link para o quarto artigo de notícias sobre o projeto",
    "Ref_5": "Link para o quinto artigo de notícias sobre o projeto",
    "Ref_6": "Link para o sexto artigo de notícias sobre o projeto",
    "Ref_7": "Link para o sétimo artigo de notícias sobre o projeto"
}

# COMMAND ----------

# Notebook separado de análises
