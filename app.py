
import logging
from flask import Flask, render_template, request, redirect, url_for, send_from_directory
import os
from telegram.ext import Updater, MessageHandler, Filters
from telegram import  Update


from scripts.config import TOKEN




os.environ['DATABASE_URL'] = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:svdb-app-web-lotobot.database.windows.net,1433;Database=db-app-web-LoToBOT;Uid=lotobot;Pwd=MC*1210lv;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

app = Flask(__name__)

logging.basicConfig(level=logging.ERROR)
logging.getLogger().setLevel(logging.ERROR)
logging.error('This is an error message')

# updater = Updater(token=TOKEN, use_context=True)
# dispatcher = updater.dispatcher

app.last_message = ''
app.last_message_fila = ''

from azure.storage.queue import QueueClient
from scripts.gerar_jogos import _gerar_jogos_fila
from scripts.config import TOKEN, CANAL_ID

import sqlalchemy as db
import urllib
import time



    # Adicione um manipulador de mensagens para receber mensagens de texto
# dispatcher.add_handler(MessageHandler(Filters.chat(CANAL_ID), _gerar_jogos))

url_queue = 'https://stappweblotobot.queue.core.windows.net/mesagens-bot'
token_sas = '?sv=2021-12-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-04-08T03:27:10Z&st=2023-04-07T19:27:10Z&spr=https,http&sig=pMMG9sZ32h3ITso%2B7Rovj0Fujr24LC%2FqHBT2RZrwcgo%3D'

queue_service = QueueClient.from_queue_url(
       queue_url=url_queue,
       credential=token_sas
   )

import json

while 1:
    messages = queue_service.receive_messages(max_messages = 1, visibility_timeout = 600)
    for message in messages:
        message_text = message.content
        print(message_text)
        status = _gerar_jogos_fila(message_text)
        print(status)
        if status == 'Arquivo enviado!':
            queue_service.delete_message(message)

        



@app.route('/')
def index():
   print('Request for index page received')
   return render_template('index.html')

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.route('/hello', methods=['POST'])
def hello():
   name = request.form.get('name')

   if name:
       print('Request for hello page received with name=%s' % name)
       return render_template('hello.html', name = name)
   else:
       print('Request for hello page received with no name or blank name -- redirecting')
       return redirect(url_for('index'))


if __name__ == '__main__':
   app.run()