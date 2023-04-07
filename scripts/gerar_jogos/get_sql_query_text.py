import sqlalchemy as db
import os
import urllib
import pandas as pd

def _get_sql_query_text(context_dict):
    
    

    Dezenas_Fixas = context_dict['Dezenas_Fixas']
    Par_Impar = context_dict['Par_Impar']
    Flag_Ultimo_jogo = context_dict['Flag_Ultimo_jogo']
    Qtd_Jogos = context_dict['Qtd_Jogos']

    cols = 'D1, D2, D3, D4, D5, D6, D7, D8, D9, D10, D11, D12, D13, D14, D15, CHAVE'


    cond_dezenas_fixas = ""
    if len(Dezenas_Fixas) > 0:
        
        for i in Dezenas_Fixas:
            cond_dezenas_fixas = cond_dezenas_fixas + f"AND B{i} = 1 "
    
    
    cond_par_impar = ''
    if (Par_Impar!='') & ('Livre' not in Par_Impar):
        Par_Impar = Par_Impar.replace('Par:','').replace('Impar:','').split('/')
        cond_par_impar = f"AND qtd_par = {Par_Impar[0]} AND qtd_impar = {Par_Impar[1]}"
    

    cond_default = ''
    if (cond_dezenas_fixas=='') & (cond_par_impar=='') & ('Sim' not in Flag_Ultimo_jogo):
        import random
        if type(Qtd_Jogos) != int:
            Qtd_Jogos = int(Qtd_Jogos.replace('Qtd: ',''))
        ids = random.sample(range(1, 3000001), Qtd_Jogos)
        ids = ', '.join(str(id) for id in ids)
        

        cond_default = f'AND id IN ({ids})'
    
    group_by_sum = ''
    if 'Sim' in Flag_Ultimo_jogo:

        
        last_df = "SELECT TOP 1 B1,B2,B3,B4,B5,B6,B7,B8,B9,B10,B11,B12,B13,B14,B15,B16,B17,B18,B19,B20,B21,B22,B23,B24,B25 FROM concursos ORDER BY id DESC"
        sql = db.text(last_df)

    
        connection_string = 'mssql+pyodbc:///?odbc_connect={}'.format(urllib.parse.quote_plus(os.environ['DATABASE_URL']))
        engine = db.create_engine(connection_string, echo=True)
        connection = engine.connect()

        result = connection.execute(sql).fetchall()
        connection.close()

        df = pd.DataFrame(result, columns=['B1','B2','B3','B4','B5','B6','B7','B8','B9','B10','B11','B12','B13','B14','B15','B16','B17','B18','B19','B20','B21','B22','B23','B24','B25'])

        dict_df = df.to_dict(orient='records')[0]
        cond_sum = []
        
        for key, value in dict_df.items():
            if ('B' in key) & (value == 1):
                cond_sum.append(f'{key} * {int(value)}')
        cond_sum = ' + '.join(cond_sum)
        
        group_by_sum = f'AND {cond_sum} >= 11'

    
    sql = f"""SELECT {cols} FROM base  WHERE id > 0 {cond_par_impar} {cond_dezenas_fixas} {cond_default} {group_by_sum}"""
    
    return sql




