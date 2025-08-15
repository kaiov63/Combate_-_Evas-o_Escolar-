import pandas as pd
import matplotlib.pyplot as plt


def ler_dtb(file: str):
    '''
    Função para leitura dos dados da Divisão Geográfica do Brasil (DTB) - Municípios 
    '''
    df_dtb = pd.read_excel(file, skiprows=6,
                           usecols=['UF', 'Nome_UF', 'Nome Região Geográfica Imediata',
                                    'Código Município Completo', 'Nome_Município'])

    df_dtb.columns = ['id_uf', 'nome_uf', 'nome_rgi', 'id_municipio', 'nome_municipio']
    df_dtb = df_dtb.drop_duplicates(subset=['id_municipio'])

    print(f'INSPEÇÃO PRIMEIRAS LINHAS\n', ':.:'*10, '\n', df_dtb.head(), '\n')
    print('INSPEÇÃO ULTIMAS LINHAS \n', ':.:'*10, '\n', df_dtb.tail(), '\n')
    print('INFO \n', ':.:'*10, '\n', df_dtb.info())

    return df_dtb


def ler_proj_IA(file: str):
    '''
    Função para leitura dos dados do arquivo Projetos IA
    '''
    aba_excel = '2024'
    df_IA = pd.read_excel(file, sheet_name=aba_excel, skiprows=5)
    
    df_IA = df_IA[['CIDADES', 'UF', 'Nº \nProjetos.7', 'Nº \nInstituições.1', 
                   'Nº \nBeneficiados.4']]
    
    df_IA.columns = ['nome_municipio', 'sg_uf', 'nr_projetos', 'nr_instituicoes', 
                   'nr_beneficiados']
    
    print(f'INSPEÇÃO PRIMEIRAS LINHAS\n', ':.:'*10, '\n', df_IA.head(), '\n')
    print('INFO \n', ':.:'*10, '\n', df_IA.info())

    return df_IA


def ler_ideb(file: str):
    '''
    Função para leitura dos dados do Índice de Desenvolvimento da Educação Básica (Ideb)
    '''
    lista_ideb = [f'VL_OBSERVADO_{x}' for x in range(2005, 2025, 2)]
    nomes_ideb = [f'ideb_{x}' for x in range(2005, 2025, 2)]

    df_ideb = pd.read_excel(file, skiprows=9, usecols=['CO_MUNICIPIO', 'REDE'] + lista_ideb, na_values=['-', '--'])
    df_ideb.columns = ['id_municipio', 'rede'] + nomes_ideb

    print(f'INSPEÇÃO PRIMEIRAS LINHAS\n', ':.:'*10, '\n', df_ideb.head(), '\n')
    print('INFO \n', ':.:'*10, '\n', df_ideb.info())

    return df_ideb


# --- CHAMANDO AS FUNÇÕES E CRIANDO AS VARIÁVEIS ---
# ATENÇÃO: As variáveis 'df_dtb_data', 'df_IA_data' e 'df_ideb_data' são criadas aqui.
# A linha que chama ler_dtb foi corrigida para salvar o resultado em uma variável.
df_dtb_data = ler_dtb(r'C:\Users\Bianca\OneDrive\Desktop\Documentos\PROJETO ALPARGATAS\data_raw\DTB_2024\RELATORIO_DTB_BRASIL_2024_DISTRITOS.xls')

# Esta linha já estava correta.
df_IA_data = ler_proj_IA(r"C:\Users\Bianca\OneDrive\Desktop\Documentos\PROJETO ALPARGATAS\data_raw\proj_IA\Projetos_de_Atuacao_-_IA_-_2020_a_2025.xlsx")

# Esta linha foi corrigida para salvar o resultado em uma variável.
df_ideb_data = ler_ideb(r'C:\Users\Bianca\OneDrive\Desktop\Documentos\PROJETO ALPARGATAS\data_raw\IDEB\divulgacao_anos_iniciais_municipios_2023.xlsx')


# --- ETAPA 1: UNIR OS DATAFRAMES (APÓS A LEITURA DOS ARQUIVOS) ---

# Limpeza e padronização dos dados antes da união

# 1. Padronizar a coluna 'nome_municipio' em ambos os DataFrames
df_IA_data['nome_municipio'] = df_IA_data['nome_municipio'].str.strip().str.upper()
df_dtb_data['nome_municipio'] = df_dtb_data['nome_municipio'].str.strip().str.upper()

# 2. Padronizar a coluna 'id_municipio' para que ambos os DataFrames tenham o mesmo tipo
# O IDEB tem IDs como float, então vamos convertê-los para inteiro
df_ideb_data['id_municipio'] = df_ideb_data['id_municipio'].fillna(0).astype(int)

# Agora, vamos tentar a união novamente com os dados limpos
df_projetos_municipios = pd.merge(df_IA_data, df_dtb_data, on='nome_municipio', how='left')

# Verificamos se a união deu certo antes de prosseguir
print("Tamanho do DataFrame de projetos e municípios após a primeira união:", df_projetos_municipios.shape)

# Unir o resultado com df_ideb_data usando o 'id_municipio' como chave
df_combinado = pd.merge(df_projetos_municipios, df_ideb_data, on='id_municipio', how='inner')

# Verificamos se a segunda união deu certo
print("Tamanho do DataFrame final após a segunda união:", df_combinado.shape)

# --- ETAPA 2: ANÁLISE E PREPARAÇÃO PARA O GRÁFICO ---

# Crie uma nova coluna para identificar se o município tem projetos ou não
df_combinado['tem_projeto'] = df_combinado['nr_projetos'] > 0

# Agrupe os dados por rede e se tem projeto para calcular a média do IDEB de 2023
media_ideb_por_grupo = df_combinado.groupby(['tem_projeto', 'rede'])['ideb_2023'].mean().reset_index()

# Renomeie a coluna booleana para algo mais legível para o gráfico
media_ideb_por_grupo['tem_projeto'] = media_ideb_por_grupo['tem_projeto'].replace({True: 'Com Projeto', False: 'Sem Projeto'})

# --- ETAPA 3: GERAR O NOVO GRÁFICO ---

import numpy as np

fig, ax = plt.subplots(figsize=(12, 7))

redes = media_ideb_por_grupo['rede'].unique()
x = np.arange(len(redes))

width = 0.35
# Barras para municípios SEM projetos
sem_projeto_data = media_ideb_por_grupo[media_ideb_por_grupo['tem_projeto'] == 'Sem Projeto']['ideb_2023']
ax.bar(x - width/2, sem_projeto_data, width, label='Sem Projeto')

# Barras para municípios COM projetos
com_projeto_data = media_ideb_por_grupo[media_ideb_por_grupo['tem_projeto'] == 'Com Projeto']['ideb_2023']
ax.bar(x + width/2, com_projeto_data, width, label='Com Projeto')

ax.set_ylabel('Média do IDEB')
ax.set_title('Média do IDEB (2023) por Rede de Ensino e Presença de Projetos')
ax.set_xticks(x)
ax.set_xticklabels(redes)
ax.legend()

plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()