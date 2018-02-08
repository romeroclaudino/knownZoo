from flask import request, Flask, Response, app
from pandas.io.json import loads,dumps
import MySQLdb
from flask_httpauth import HTTPBasicAuth
from functools import wraps
import json

app=Flask(__name__)

conn = MySQLdb.connect(host= "localhost",
                  user="root",
                  passwd="root",
                  db="knownzoo")

cursor= conn.cursor()

def check_auth(username,password):
    return username=="admin" and password=="admin"

def authenticate():
    return Response('Could not verify your access level for that URL.\n'
    'You have to login with proper credentials', 401,
    {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decoreted(*args,**kwargs):
        auth= request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args,**kwargs)
    return decoreted


def insertOnDataBase(recinto, mac, momento_chegada,momento_saida,tempo_permanencia):
    query="insert into dados (RecintoId,Mac,MomentoEntrada,MomentoSaida,Permanencia) values \
            ('%s',%s,'%s','%s','%s');"%(recinto,mac,momento_chegada,momento_saida,tempo_permanencia)
            
    cursor.execute(query)
    conn.commit()
    
def quantidadePorRecinto(inicio, fim):
     
    query="SELECT Distinct recinto.Nome, SUM(1) Populacao \
    FROM dados JOIN recinto ON dados.RecintoId = recinto.Id \
    WHERE MomentoEntrada >= '%s' \
    AND MomentoSaida <= '%s' \
    GROUP BY RecintoId;" %(inicio,fim)
    
    cursor.execute(query)
    
    row=cursor.fetchone()

    dados={"dados":[]}
    
    while row:
        temp_dic={"recinto": row[0],
                  "populacao": int(row[1])}
        dados['dados'].append(temp_dic)
        row=cursor.fetchone()
        
    return dados
    
def quantidadeRecintoTempo(recinto,inicio,fim):
    
    query="SELECT Distinct recinto.Nome, SUM(1) Populacao \
    FROM dados JOIN recinto ON dados.RecintoId = recinto.Id \
    WHERE MomentoEntrada >= '%s' \
    AND MomentoSaida <= '%s'\
    AND RecintoId=%s;" %(inicio,fim,recinto)
    cursor.execute(query)
    
    row=cursor.fetchone()
    dados={'dados':[]}
    while row:
        
        temp_dic={'intervalo': row[0],
                  'quantidade': row[1]}
        
        dados['dados'].append(temp_dic)
        
        row=cursor.fetchone()
    return dados

def mediaPermanenciaRecintos():
    
    query="SELECT Distinct recinto.Nome, AVG(Permanencia) Populacao \
    FROM dados JOIN recinto ON dados.RecintoId = recinto.Id group by RecintoId ;"
    
    cursor.execute(query)
    
    row=cursor.fetchone()

    dados={"dados":[]}
    
    while row:
        temp_dic={"recinto": row[0],
                  "permanencia": int(row[1])}
        dados['dados'].append(temp_dic)
        row=cursor.fetchone()
        
    return dados

def mediaPermanenciaRecinto(recintoId):
    query="SELECT Distinct recinto.Nome, AVG(Permanencia) Populacao \
    FROM dados JOIN recinto ON dados.RecintoId = recinto.Id where RecintoId=%s ;"%(recintoId)
    
    cursor.execute(query)
    
    row=cursor.fetchone()

    dados={"dados":[]}
    
    while row:
        temp_dic={"recinto": row[0],
                  "permanencia": int(row[1])}
        dados['dados'].append(temp_dic)
        row=cursor.fetchone()
        
    return dados


@app.route("/individuo",methods=["POST"])
@requires_auth
def inserirNoBanco():
    if "POST":
        jsons = request.get_data()
        #print("=================="+str(jsons)+"============================")
        # decodedJson = json.JSONDecoder().decode(jsons)
        decodedJson = jsons.decode('utf-8')
        print(decodedJson)
        individuo = json.loads(decodedJson)
        print("==================>"+individuo)
        insertOnDataBase(individuo['idRecinto'],\
                         individuo['mac'], \
                         individuo['momentoChegada'], \
                         individuo['momentoSaida'], \
                         individuo['tempoPermanencia'])
        
        return status.HTTP_200_OK

@app.route("/recintos",methods=["POST"])
@requires_auth
def getQuantidadePorRecinto():
    json = request.get_data()
    dic_json = loads(json.decode("utf-8"))
    
    inicio  = dic_json['inicio']
    fim     = dic_json['fim']
    
    dic_retorno = quantidadePorRecinto(inicio, fim)
    return dumps(dic_retorno)
    
@app.route("/recinto/tempo",methods=["POST"])
@requires_auth
def getQuantidadeRecintoTempo():
    json    = request.get_data()
    dic_json= loads(json.decode("utf-8"))
    recinto = dic_json['recinto']
    inicio  = dic_json['inicio']
    fim     = dic_json['fim']
    
    dic_retorno = quantidadeRecintoTempo(recinto, inicio, fim)
    
    return dumps(dic_retorno)

@app.route("/recintos/mediapermanencia",methods=["POST"])
@requires_auth
def getMediaRecintosPermanencia():
    
    dic_retorno = mediaPermanenciaRecintos()
    return dumps(dic_retorno)
    
@app.route("/recinto/mediapermanencia",methods=["POST"])
@requires_auth
def getMediaRecintoPermanencia():
    
    json    = request.get_data()
    dic_json= loads(json.decode("utf-8"))
    recinto = dic_json['recinto']
    dic_retorno = mediaPermanenciaRecinto(str(recinto))
    return dumps(dic_retorno)


if __name__=="__main__":
    app.run(debug=True)
