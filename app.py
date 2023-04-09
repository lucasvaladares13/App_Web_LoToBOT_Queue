
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

updater = Updater(token=TOKEN, use_context=True)
dispatcher = updater.dispatcher

app.last_message = ''
app.last_message_fila = ''

from scripts.gerar_jogos import _gerar_jogos
from scripts.config import TOKEN, CANAL_ID
    # Adicione um manipulador de mensagens para receber mensagens de texto
dispatcher.add_handler(MessageHandler(Filters.chat(CANAL_ID), _gerar_jogos))



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

@app.route(f'/{TOKEN}', methods=['POST'])
def recive_message():
   print('ok')
   json_data = request.get_json()
   update = Update.de_json(json_data, updater.bot)
   dispatcher.process_update(update)
    
   app.last_message = str(json_data)
   return 'ok'
 
@app.route('/mensagem')
def mensagem():
    return render_template('mensagem.html', mensagem=app.last_message)

@app.route('/receberfila', methods=['POST'])
def receber_fila():
    json_data = request.get_json()
    app.last_message_fila = json_data
    return 200



@app.route('/mensagemfila')
def mensagem_fila():
    
    return render_template('mensagem.html', mensagem=app.last_message_fila)




if __name__ == '__main__':
   app.run()