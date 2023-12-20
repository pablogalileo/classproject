from flask import Flask, request, jsonify
import pandas as pd
from datetime import datetime

from pycaret.regression import load_model
from pycaret.regression import predict_model

rf_model = load_model('../models/model_v1')
dt_model = load_model('../models/model_v2')
et_model = load_model('../models/model_v3')

app = Flask(__name__)

@app.route('/hello', methods=['GET'])
def saludo():
    try:
        a = request.args.get('a',None)
        strOut = "Hola mundo: {a}"
        return jsonify({'mensaje': strOut})
    except:
        return jsonify({'resultado':'No se enviaron los parametros para operar.'})

# MODELO RANDON FOREST
@app.route('/predict_rf/', methods=['POST'])
def predict_rf():
    data = request.json
    data_to_predict = pd.json_normalize(data) #convertirmos a dataframe con los tipos de datos default
    try:
        prediction = predict_model(rf_model, data=data_to_predict)
        valor_predicho = round(list(prediction['prediction_label'])[0], 0)

        with open('model_logs.log', 'a') as file_modificado:
            #esribir el contenido modificado en el file
            strLog= f'Modelo: Randon Forest - Predicted_Value: {valor_predicho} - Date: {datetime.now().strftime("%Y-%m-%d %H:%M%S")}\n'
            file_modificado.write(strLog)

        print(valor_predicho)
        return jsonify({'Prediction': valor_predicho})
    
    except:
        with open('model_logs.log', 'a') as file_modificado:
            #esribir el contenido modificado en el file
            strLog= f'Error, at Date: {datetime.now().strftime("%Y-%m-%d %H:%M%S")}, at model: Randon Forest\n'
            file_modificado.write(strLog)
        return jsonify({'mensaje': 'Se genero un error en la prediction'})
    
# MODELO ALGORITO ARBOLES DE DESICION
@app.route('/predict_dt/', methods=['POST'])
def predict_dt():
    data = request.json
    data_to_predict = pd.json_normalize(data) #convertirmos a dataframe con los tipos de datos default
    try:
        prediction = predict_model(dt_model, data=data_to_predict)
        valor_predicho = round(list(prediction['prediction_label'])[0], 0)

        with open('model_logs.log', 'a') as file_modificado:
            #esribir el contenido modificado en el file
            strLog= f'Modelo: Arboles Desicion - Predicted_Value: {valor_predicho} - Date: {datetime.now().strftime("%Y-%m-%d %H:%M%S")}\n'
            file_modificado.write(strLog)

        print(valor_predicho)
        return jsonify({'Prediction': valor_predicho})
    
    except:
        with open('model_logs.log', 'a') as file_modificado:
            #esribir el contenido modificado en el file
            strLog= f'Error, at Date: {datetime.now().strftime("%Y-%m-%d %H:%M%S")}, at model: Arboles Desicion \n'
            file_modificado.write(strLog)
        return jsonify({'mensaje': 'Se genero un error en la prediction'})


#MODELO ALGORITMO ARBOLES EXTRA DESICION
@app.route('/predict_et/', methods=['POST'])
def predict_et():
    data = request.json
    data_to_predict = pd.json_normalize(data) #convertirmos a dataframe con los tipos de datos default
    try:
        prediction = predict_model(et_model, data=data_to_predict)
        valor_predicho = round(list(prediction['prediction_label'])[0], 0)

        with open('model_logs.log', 'a') as file_modificado:
            #esribir el contenido modificado en el file
            strLog= f'Modelo: Extra Tree Desition - Predicted_Value: {valor_predicho} - Date: {datetime.now().strftime("%Y-%m-%d %H:%M%S")}\n'
            file_modificado.write(strLog)

        print(valor_predicho)
        return jsonify({'Prediction': valor_predicho})
    
    except:
        with open('model_logs.log', 'a') as file_modificado:
            #esribir el contenido modificado en el file
            strLog= f'Error, at Date: {datetime.now().strftime("%Y-%m-%d %H:%M%S")}, at model:  Extra Tree Desition\n'
            file_modificado.write(strLog)
        return jsonify({'mensaje': 'Se genero un error en la prediction'})

    
# MODELOS RF, DT, ET
@app.route('/predictAll/', methods=['POST'])
def predictAll():
    data = request.json
    data_to_predict = pd.json_normalize(data) #convertirmos a dataframe con los tipos de datos default
    tb = []
    try:
        prediction_rf = predict_model(rf_model, data=data_to_predict)
        vp_rf = round(list(prediction_rf['prediction_label'])[0], 0)
        prediction_dt = predict_model(dt_model, data=data_to_predict)
        vp_dt = round(list(prediction_dt['prediction_label'])[0], 0)
        prediction_et = predict_model(et_model, data=data_to_predict)
        vp_et = round(list(prediction_et['prediction_label'])[0], 0)

        with open('model_logs.log', 'a') as file_modificado:
            #esribir el contenido modificado en el file
            strLog= f'Modelo: RF, DT, ET - Predicted_Value: {vp_rf} {vp_dt} {vp_et} - Date: {datetime.now().strftime("%Y-%m-%d %H:%M%S")}\n'
            file_modificado.write(strLog)

        return jsonify({'Prediction RF': vp_rf, 'Prediction DT': vp_dt, 'Prediction ET': vp_et})
    
    except:
        with open('model_logs.log', 'a') as file_modificado:
            #esribir el contenido modificado en el file
            strLog= f'Error, at Date: {datetime.now().strftime("%Y-%m-%d %H:%M%S")}, at model:  Extra Tree Desition\n'
            file_modificado.write(strLog)
        return jsonify({'mensaje': 'Se genero un error en la prediction'})