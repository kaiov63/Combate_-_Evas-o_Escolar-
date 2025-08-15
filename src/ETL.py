import os
import utils
import pandas as pd


# Função para padronizar nomes das cidades
def formatar_nome(df, coluna, novo_nome='nome_municipio_formatado'):
   df[novo_nome] = (df[coluna].str.upper()
                  .str.replace(r'[-.!?"`()]', '', regex=True)
                  .str.replace('MIXING CENTER', '')
                  .str.strip()
                  .str.replace(' ', ''))
   return df


# Adicona o diretório do usuário
diretorio_df = os.path.expanduser('~/')


# ETL DTB
file = os.path.join(diretorio_df, r'Desktop/PROJETO ALPARGATAS/data_raw/DTB_2024/RELATORIO_DTB_BRASIL_2024_DISTRITOS.xls')

print('Lendo o arquivo:', file)
df_dtb = utils.ler_dtb(file)

# Padronizando DTB
df_dtb = formatar_nome(df=df_dtb, coluna='nome_municipio')
print('IBGE \n', df_dtb.head())


# ETL PROJETOS IA
file = os.path.join(diretorio_df, r'Desktop/PROJETO ALPARGATAS/data_raw/proj_IA\Projetos_de_Atuacao_-_IA_-_2020_a_2025.xlsx')

print('Lendo o arquivo:', file)
df_IA = utils.ler_proj_IA(file)

# Padronizando Projetos IA
df_IA = formatar_nome(df=df_IA, coluna='nome_municipio')
df_IA['nome_uf'] = df_IA['sg_uf'].map({'PB': 'Paraíba', 'PE': 'Pernambuco',
                                    'MG': 'Minas Gerais', 'SP': 'São Paulo'})

df_IA = df_IA[df_IA['sg_uf'].str.len() == 2]

print('IA \n', df_IA.head())
print('IA \n', df_IA.sg_uf.value_counts())


# MESCLAGEM DOS DFs
data_m = df_IA.merge(df_dtb, how='left', on=['nome_municipio_formatado', 'nome_uf'],
                     suffixes=['_ia', ''], indicator='tipo_merge')

print(data_m.head())
print(data_m['tipo_merge'].value_counts)

'''
how="inner": Tipo de junção. Um inner join significa que a nova tabela (data_m) conterá 
apenas as linhas onde os valores das colunas de junção existem em ambos os DataFrames. 
Se uma cidade estiver apenas na lista de projetos ou apenas no DTB, ela não será incluída 
no data_m.

on= : Especifica as colunas que serão usadas como chaves de junção. O pandas vai procurar 
por linhas que tenham a mesma combinação de valores tanto em nome_municipio quanto em nome_uf 
nos dois DataFrames. É por isso que é feito todo o trabalho de formatar os nomes das cidades
antes, para garantir que a junção funcione corretamente.

suffixes= : Renomeia colunas com o mesmo nome que existem nos dois DataFrames. 
Exemplo: se ambos tivessem uma coluna 'id_uf', a do data viraria 'id_uf_ia' e a do df_dtb 
continuaria 'id_uf'. Isso evita conflitos de nomes após a junção.

indicator="tipo_merge": Cria uma nova coluna no DataFrame final chamada tipo_merge.
Essa coluna é muito útil para depuração, pois ela indica a origem de cada linha:

   both: A linha foi encontrada em ambos os DataFrames (data e df_dtb).
   left_only: A linha só foi encontrada no DataFrame da esquerda (data).
   right_only: A linha só foi encontrada no DataFrame da direita (df_dtb).
'''

# Investiga dados que não foram encontrados na junção
print('Projetos de IA que não foram encontrados na base de dados do IBGE:')
n_encontrados_dtb = data_m.query('tipo_merge=="left_only"')  # query(): filtra linhas do df
print(n_encontrados_dtb.head())


# IDEB
file = os.path.join(diretorio_df, r'Desktop/PROJETO ALPARGATAS/data_raw/IDEB/divulgacao_anos_iniciais_municipios_2023.xlsx')

print('Lendo o arquivo:', file)
df_ideb = utils.ler_ideb(file)

data_final = data_m.merge(df_ideb, how='left')
print(data_final)
