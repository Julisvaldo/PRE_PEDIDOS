import datetime as dt
import requests
import json
import pandas as pd
import os

#LER ARQUIVO IMPORTADO, FAZER O AGRUPAMENTO E REMOVER DUPLICADAS


pasta = 'C:\Users\julio.lima\Documents\PROJETO_PRE_PEDIDOS\\SUGESTÃO DE VENDAS - 19-10-2023.xlsx'

df_base = pd.read_excel(pasta)

df_base = df_base[['FILIAL', 'CODSUPERVISOR','Codigo', 'Data Início', 'Data Final']]\
        .sort_values(by='Codigo')\
        .drop_duplicates(subset=['Codigo', 'Data Início', 'Data Final', 'CODSUPERVISOR', 'FILIAL'])

num_linhas = len(df_base)

df = pd.read_excel(pasta)

#LER DATA DE HOJE E TRAZER NÚMERO CORRESPONDENTE

data_referencia = dt.datetime(2023,1,1)
data_atual = dt.datetime.now()
dia = (data_atual - data_referencia).days + 1

#CRIAR E ADICIONAR COLUNAS COM VARIÁVEIS FIXAS E O ID(DATA)

vMesDia = [290] * num_linhas
vcor = ['#0e3cde'] * num_linhas
vordenaritens = [''] * num_linhas
vordenaritenspopup = ['S'] * num_linhas
vsequenciaSC = list(range(1, num_linhas + 1))

df_base['MesDia'] = vMesDia
df_base['cor'] = vcor
df_base['ordenaritens'] = vordenaritens
df_base['ordenaritenspopup'] = vordenaritenspopup
df_base['sequenciaSC'] = vsequenciaSC
df_base['descricao'] = 'Pre-pedido via API com cor - Cliente ' + df_base['Codigo'].astype(str)

df_base['ID'] = df_base['MesDia'].astype(str) + df_base['sequenciaSC'].astype(str)

#RENOMEAR CONFORME A JOB DO PENTAHO

df_base = df_base.rename(columns={'Codigo': 'codcli', 'FILIAL': 'codfilial', 'ID':'codigo', 'Data Início': 'dtinicio', 'Data Final': 'dtfim', 'CODSUPERVISOR': 'codsupervisor'})

df_base['dtinicio'] = df_base['dtinicio'].dt.strftime('%Y-%m-%d')

df_base['dtfim'] = df_base['dtfim'].dt.strftime('%Y-%m-%d')

#IDENTIFICAR CADA REQUEST, ORGANIZAR EM DATAFRAMES, E TRANSFORMAR EM JSON 

df_cabecalho = df_base[['codigo','descricao', 'dtinicio', 'dtfim', 'cor', 'ordenaritens', 'ordenaritenspopup']]
js_cabecalho = df_cabecalho.to_json(orient = 'records')

df_filial = df_base[['codigo', 'codfilial']]
df_filial = df_filial.rename(columns={'codigo':'codprepedido'})
js_filial = df_filial.to_json(orient = 'records')

df_supervisor = df_base[['codigo', 'codsupervisor']]
df_supervisor = df_supervisor.rename(columns={'codigo':'codprepedido'})
js_supervisor = df_supervisor.to_json(orient = 'records')

df_cliente = df_base[['codigo', 'codcli']]
df_cliente = df_cliente.rename(columns={'codigo':'codprepedido'})
js_cliente = df_cliente.to_json(orient = 'records')

df_itens = df[['Codigo', 'CODPROD', 'Soma de Qt Max Pedid 60d']]\
        .sort_values(by='Codigo')\
        .rename(columns={'Codigo': 'codcli', 'CODPROD': 'codprod', 'Soma de Qt Max Pedid 60d': 'quantidade'})\
        .merge(df_cliente, on=['codcli','codcli'], how='inner')\
        .sort_values(by='codcli', ascending=True)\
        .drop(columns=['codcli'])
        
df_itens = df_itens[['codprepedido', 'codprod', 'quantidade']].astype(str)
js_itens = df_itens.to_json(orient='records')



#GERANDO TOKEN PARA OS ENDPOINTS

def token():

       url_token = "https://intext-hmg.solucoesmaxima.com.br:81/api/v3/Login"

       payload = json.dumps({
       "login": "WV8QKSJFPRAC2Wh/jkkKIMi5MpOJ7A89PoXL412anX4=",
       "password": "XC9D2SWJnGArIQ/iLhUE/UwtprTApXfQWDyNkTCyJRU="
       })
       headers = {
       'Content-Type': 'application/json'
       }

       response_token = requests.request("POST", url_token, headers=headers, data=payload)
       
       data = json.loads(response_token.content)

       return 'Bearer ' + ( data['token_De_Acesso'])

#ALIMENTANDO OS ENDPOINTS COM OS JSON GERADOS

def endpoint_cabecalho(body):
       url = "https://intext-hmg.solucoesmaxima.com.br:81/api/v1/PrePedidos"
       headers = {
              'Content-Type': 'application/json',
              'Authorization': str(token())
       }
      
       response = requests.request("POST", url, headers=headers, data=body)
       return response.status_code
       
resposta_cabecalho = endpoint_cabecalho(js_cabecalho)
print('resposta solicitação cabecalho:' + str(resposta_cabecalho))



def endpoint_supervisor(body1):
       url = "https://intext-hmg.solucoesmaxima.com.br:81/api/v1/PrePedidosSupervisores"
       headers = {
              'Content-Type': 'application/json',
              'Authorization': str(token())
       }
      
       response = requests.request("POST", url, headers=headers, data=body1)
       
       return response.status_code

resposta_supervisor = endpoint_supervisor(js_supervisor)
print('resposta solicitação supervisor:' + str(resposta_supervisor))



def endpoint_filial(body2):
       url = "https://intext-hmg.solucoesmaxima.com.br:81/api/v1/PrePedidosFiliais"
       headers = {
              'Content-Type': 'application/json',
              'Authorization': str(token())
       }
      
       response = requests.request("POST", url, headers=headers, data=body2)
       
       return response.status_code

resposta_filial = endpoint_filial(js_filial)
print('resposta solicitação filial:' + str(resposta_filial))



def endpoint_cliente (body3):
       url = "https://intext-hmg.solucoesmaxima.com.br:81/api/v1/PrePedidosClientes"
       headers = {
              'Content-Type': 'application/json',
              'Authorization': str(token())
       }
      
       response = requests.request("POST", url, headers=headers, data=body3)
       
       return response.status_code

resposta_cliente = endpoint_cliente(js_cliente)
print('resposta solicitação cliente:' + str(resposta_cliente))



def endpoint_itens (body4):
       url = "https://intext-hmg.solucoesmaxima.com.br:81/api/v1/PrePedidosItens"
       headers = {
              'Content-Type': 'application/json',
              'Authorization': str(token())
       }
      
       response = requests.request("POST", url, headers=headers, data=body4)
       
       return response.status_code

resposta_itens = endpoint_itens(js_itens)
print('resposta solicitação itens:' + str(resposta_itens))