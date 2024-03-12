import pandas as pd 
import json
import random 
import time
import hashlib


from kafka import KafkaProducer
from confluent_kafka import Producer
import socket
import json
import os

from kafka_auth import conf

# Leer el archivo CSV
df = pd.read_csv('Sales_simulation.csv', usecols=['Category', 'Name', 'Purchase Price', 'Delivery Price'])


cities = ['Bogotá','Medellín','Cali','Bucaramanga','Barranquilla']
payment_online = ['Credit_card','PSE']
payment_store = ['Cash','Nequi','Daviplata','Credit_card']
source = ['Facebook','Instagram','Organic','Twitter','Influencer_1','Influencer_2','Influencer_3','Influencer_4']
status_purchase = ['COMPLETED','FAILED_CHECKOUT','FAILED_API_RESPONSE','INSUFICCIENT_FUNDS','COMPLETED','COMPLETED','COMPLETED','COMPLETED','COMPLETED','COMPLETED','FAILED_API_RESPONSE','INSUFICCIENT_FUNDS','USER_ERROR','FRAUD','COMPLETED','COMPLETED','COMPLETED']

bog_coords = [(4.651600960108946, -74.12628850377475), (4.661984763443271, -74.13466548843223), (4.647569693587232, -74.10186525959672)]
buc_coords = [(7.099917472863198, -73.10730617492302), (7.07243688331635, -73.10525936227518)]
cali_coords = [(3.4287786094866717, -76.53749228193497), (3.4148803158142966, -76.54041613631288), (3.4164091379805432, -76.54751692551635)]
bar_coords = [(11.014167139030025, -74.82747678131524), (11.004041676077495, -74.83545204769058), (10.990580146449894, -74.78876005521157)]
mede_coords = [(6.163315879042265, -75.6052691935286), (6.17784573272914, -75.59141059178306), (6.198053256721469, -75.5733524126965)]

# Función para obtener los créditos a favor del usuario
def get_user_credits():
    return random.choice([0, 2000, 5000, 10000, 15000, 20000])


def get_coords(city):
    
    if city == 'Bogotá' :
        coords = random.choice(bog_coords)
    elif city == 'Medellín' :
        coords = random.choice(mede_coords)
    elif city == 'Cali' :
        coords = random.choice(cali_coords)
    elif city == 'Bucaramanga' :
        coords = random.choice(buc_coords)
    elif city == 'Barranquilla' :
        coords = random.choice(bar_coords)
        
    return coords 

def get_pay_method(source,status_purchase,payment_online,payment_store):
    
    if source == 'Organic' :
        
        payment = random.choice(payment_store)
        status = 'COMPLETED'
        order_type = 'STORE'
    
    elif source != 'Organic' :
        
        payment = random.choice(payment_online)
        status = random.choice(status_purchase)
        order_type = 'ONLINE'
    
    return payment,status,order_type   

    # Función para obtener la propina para el domiciliario
def get_tip():
    return random.choice([0, 2000, 3000, 5000])


x = 0

data_purchase = []

while(x < 10):
    date = pd.to_datetime('today').strftime("%Y-%m-%d %H:%M:%S")  # Obtiene la fecha y hora actual
    
    for index, row in df.iterrows():  # Itera a través de cada fila del DataFrame df
        product = row['Name']  # Obtiene el valor de la columna 'Name' de la fila actual
        pricing = row['Purchase Price']  # Obtiene el valor de la columna 'Purchase Price' de la fila actual
        delivery_price = row['Delivery Price']  # Obtiene el valor de la columna 'Delivery Price' de la fila actual
        category = row['Category']  # Obtiene el valor de la columna 'Category' de la fila actual
        source_temp = random.choice(source)  # Elige aleatoriamente una fuente de compra de la lista de fuentes disponibles
        pay = get_pay_method(source_temp, status_purchase, payment_online, payment_store)  # Determina el método de pago, el estado de la compra y el tipo de orden
        city = random.choice(cities)  # Elige aleatoriamente una ciudad de la lista de ciudades disponibles
        
        # Obtiene los créditos a favor del usuario y la propina para el domiciliario
        user_credits = get_user_credits()
        tip = get_tip()
        
        # Calcula el tiempo de entrega estimado
        delivery_time = random.randint(15, 60)  # Estimación del tiempo de entrega en minutos
        
        # Crea un diccionario con la información de la compra
        purchase = {
            'purchase_ID': str(hashlib.sha256(f"{index} {product} {pricing} {delivery_price} {date} {source_temp} {pay[1]}".encode('utf-8')).hexdigest())[:10],
            'Product_name': product,
            'Pricing': str(pricing),
            'Delivery_Price': str(delivery_price),
            'Category': category,
            'Payment_Method': pay[0],
            'Status': pay[1],
            'Order_Type': pay[2],
            'City': city,
            'Latitud': str(get_coords(city)[0]),
            'Longitud': str(get_coords(city)[1]),
            'Source': source_temp,
            'User_Credits': user_credits,
            'Tip': tip,
            'Estimated_Delivery_Time': delivery_time,  # Tiempo de entrega estimado en minutos
            'Created_at': date
        }
        
        # Agrega la información de la compra al lista de datos de compra
     
        data_purchase.append(pd.DataFrame(purchase,index=[x]))

        print(purchase)
        x += 1  # Imprime la información de la compra
        time.sleep(random.choice([1, 2]))  # Espera un tiempo aleatorio antes de continuar con la siguiente iteración
