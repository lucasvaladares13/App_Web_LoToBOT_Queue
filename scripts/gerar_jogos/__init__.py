from io import BytesIO
from .get_sql_query_text import _get_sql_query_text
import sqlalchemy as db
import urllib
from datetime import date
from scripts.config import CANAL_ID
import os
import pandas as pd


def _gerar_jogos(update, context):
    

    context_dict = init_user_data()

    mensagem = update.channel_post.text
    print(mensagem)
    mensagem = mensagem.split(';')
    print(mensagem)
    context_dict['user_id'] = mensagem[0]
    context_dict['Dezenas_Fixas'] = [] if mensagem[1] == '[]' else mensagem[1].replace("[","").replace("]","").replace(" ","").split(',')
    context_dict['Par_Impar'] = mensagem[2]
    context_dict['Flag_Ultimo_jogo'] = mensagem[3]
    context_dict['Qtd_Jogos'] = mensagem[4]

    
    qtd_jogos = context_dict['Qtd_Jogos']
    if qtd_jogos != 10:
        qtd_jogos = qtd_jogos.replace('Qtd: ','')
    sql = _get_sql_query_text(context_dict)
    sql = db.text(sql)

    connection_string = 'mssql+pyodbc:///?odbc_connect={}'.format(urllib.parse.quote_plus(os.environ['DATABASE_URL']))
    engine = db.create_engine(connection_string, echo=True)
    connection = engine.connect()

    result = connection.execute(sql).fetchall()
    connection.close()

    df = pd.DataFrame(result, columns=['D1', 'D2', 'D3', 'D4', 'D5', 'D6', 'D7', 'D8', 'D9', 'D10', 'D11', 'D12', 'D13', 'D14', 'D15', 'CHAVE'])



    
    df = df.sample(int(qtd_jogos))
    
    #df = export_user_data(context)
    # Cria um objeto BytesIO para armazenar o arquivo em memória
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, sheet_name='Sheet1', index=False)
    writer.save()
    output.seek(0)


    data = date.today()
    data_em_texto = data.strftime('%d_%m_%Y')
    
    context.bot.send_document(chat_id=CANAL_ID, document=output, filename=f'{context_dict["user_id"]}-Jogos_LotoFacil_{data_em_texto}.xlsx')

def _gerar_jogos_fila(mensagem):
    import telegram
    from scripts.config import TOKEN_WORKER, CANAL_ID

    context_dict = init_user_data()
    
    print(mensagem)
    mensagem = mensagem.split(';')
    print(mensagem)
    context_dict['user_id'] = mensagem[0]
    context_dict['Dezenas_Fixas'] = [] if mensagem[1] == '[]' else mensagem[1].replace("[","").replace("]","").replace(" ","").split(',')
    context_dict['Par_Impar'] = mensagem[2]
    context_dict['Flag_Ultimo_jogo'] = mensagem[3]
    context_dict['Qtd_Jogos'] = mensagem[4]

    
    qtd_jogos = context_dict['Qtd_Jogos']
    if qtd_jogos != 10:
        qtd_jogos = qtd_jogos.replace('Qtd: ','')
    sql = _get_sql_query_text(context_dict)
    sql = db.text(sql)

    connection_string = 'mssql+pyodbc:///?odbc_connect={}'.format(urllib.parse.quote_plus(os.environ['DATABASE_URL']))
    engine = db.create_engine(connection_string, echo=True)
    connection = engine.connect()

    result = connection.execute(sql).fetchall()
    connection.close()

    df = pd.DataFrame(result, columns=['D1', 'D2', 'D3', 'D4', 'D5', 'D6', 'D7', 'D8', 'D9', 'D10', 'D11', 'D12', 'D13', 'D14', 'D15', 'CHAVE'])



    
    df = df.sample(int(qtd_jogos))
    
    #df = export_user_data(context)
    # Cria um objeto BytesIO para armazenar o arquivo em memória
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, sheet_name='Sheet1', index=False)
    writer.save()
    output.seek(0)

    status = 1
    while status == 1:
        try:
            data = date.today()
            data_em_texto = data.strftime('%d_%m_%Y')
            bot = telegram.Bot(token=TOKEN_WORKER)

            # Envia a mensagem de teste
            bot.send_document(chat_id=CANAL_ID, document=output, filename=f'{context_dict["user_id"]}-Jogos_LotoFacil_{data_em_texto}.xlsx')
            status = 0
        except:
            print( 'Tentando enviar denovo!')
    
    return 'Arquivo enviado!'



def init_user_data():
    context_dict = {}
    context_dict['Dezenas_Fixas'] = []
    context_dict['Par_Impar'] = ''
    context_dict['Flag_Ultimo_jogo'] = ''
    context_dict['Qtd_Jogos'] = 10

    return context_dict

