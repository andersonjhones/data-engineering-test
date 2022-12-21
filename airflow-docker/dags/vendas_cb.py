from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain


#Importação das bibliotecas para processamento do dataframe
import pandas as pd
import os

#argumentos padrão do airflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(schedule_interval=None, start_date=datetime(2022, 12, 14), catchup=False, tags=['example'])
def Vendas():
    """
    Flow para soma de vendas de combustíveis e tipos de éleo diesel
    """
    @task()
    def start():
        print("iniciando...")
    

    @task()
    def pipeline_one():
        #Carregando os arquivos para dataframes
        dadosp2 = pd.read_excel('/opt/airflow/data/vendas_combustiveis.xlsx', engine="openpyxl", sheet_name='DPCache_m3')
        dadosp3 = pd.read_excel('/opt/airflow/data/vendas_combustiveis.xlsx', engine="openpyxl", sheet_name='DPCache_m3_2')
        dadosp4 = pd.read_excel('/opt/airflow/data/vendas_combustiveis.xlsx', engine="openpyxl", sheet_name='DPCache_m3_3')
        #1. Vendas de Combustíveis Derivados de Petróleo por Unidade Federativa (UF) e Produto
        agrupamento1_2 = dadosp2.groupby([dadosp2["ANO"], dadosp2["COMBUSTÍVEL"]])[['Jan', 'Fev', 'Mar', 'Abr','Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez', 'TOTAL']].sum()
        agrupamento2_2 = dadosp3.groupby([dadosp3["ANO"], dadosp3["COMBUSTÍVEL"]])[['Jan', 'Fev', 'Mar', 'Abr','Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez', 'TOTAL']].sum()
        agrupamento3_2 = dadosp4.groupby([dadosp4["ANO"], dadosp4["COMBUSTÍVEL"]])[['Jan', 'Fev', 'Mar', 'Abr','Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez', 'TOTAL']].sum()
        a=agrupamento1_2.reset_index()
        b = agrupamento2_2.reset_index()
        c= agrupamento3_2.reset_index()
        d=pd.concat([a,b,c], axis=0, ignore_index=True)
        #Mostrando o conjunto de dados agrupado por ano e combustível (produto), somando todas UFs
        e = d.groupby([d["ANO"], d["COMBUSTÍVEL"]])[['Jan', 'Fev', 'Mar', 'Abr','Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez', 'TOTAL']].sum().reset_index()
        por_ano_combustivel= d.groupby([d["ANO"], d["COMBUSTÍVEL"]])[['Jan', 'Fev', 'Mar', 'Abr','Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez', 'TOTAL']].sum().reset_index()
        #**Agrupando o conjunto de dados concatenado com o somatório total de vendas de combustíveis em cada mês de cada ano de 2000 a 2020 e os respectivos totais anuais**
        f= por_ano_combustivel.groupby([por_ano_combustivel["ANO"]])[['Jan', 'Fev', 'Mar', 'Abr','Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez', 'TOTAL']].sum()
        

        e.to_csv("./data/por_ano_e_combustivel_tuf.csv")
        # e.to_csv("/opt/airflow/data/por_ano_e_combustivel_tuf.csv")
        f.to_csv("./data/por_ano_total.csv")



    # **2. Vendas de Óleo Diesel por Unidade Federativa (UF) e Tipo**
    @task()
    def pipeline_two():
        #**Agrupamento por ano e por tipo de combustível na planilha DPCache_m3_2, pois ela têm 5 tipos de óleo diesel**
        dadosp3_q2 = pd.read_excel('/opt/airflow/data/vendas_combustiveis.xlsx', engine="openpyxl", sheet_name='DPCache_m3_2')
        agrupamento2_2_q2 = dadosp3_q2.groupby([dadosp3_q2["ANO"], dadosp3_q2["COMBUSTÍVEL"]])[['Jan', 'Fev', 'Mar', 'Abr','Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez', 'TOTAL']].sum().reset_index()
        """**Agrupando o total de vendas de óleo diesel, com o somatório dos tipos, por ano**
        Pode-se ver os totais de cada ano de 2013 a 2020 por mês e o total anual"""
        oleo_tipo_por_ano = dadosp3_q2.groupby([dadosp3_q2["ANO"]])[['Jan', 'Fev', 'Mar', 'Abr','Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez', 'TOTAL']].sum()
        
        agrupamento2_2_q2.to_csv("./data/por_ano_e_tipo.csv")
        oleo_tipo_por_ano.to_csv("./data/por_ano_total_vendas_diesel.csv")



 
    chain(start() , [pipeline_one(), pipeline_two()]
    )
dag = Vendas()
