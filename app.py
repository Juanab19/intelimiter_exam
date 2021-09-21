import pandas as pd
from peewee import *
import os
import sys
import numpy as np
import datetime


db = MySQLDatabase("intelimeter_exam", host = 'localhost', port = 3306, user = 'root', password = 'password')

class Meters(Model):

    serial_number = TextField(unique = True)
    n_meter = IntegerField()
    panel_number = TextField()
    job_number = TextField()
    job_name = TextField()
    seal = BooleanField()
    m_type = TextField()
    modbus_id = IntegerField()
    timestamp = DateTimeField(default = datetime.datetime.now)


    class Meta:
        database = db


def initialize():
    """Se realiza la conexión con la base de datos y se crean las tablas en la misma"""
    db.connect()
    db.create_tables([Meters], safe = True)

def add_entry(data_dicts):
    """Esta función registra una entrada a la base de datos"""

    try:
        for dic in data_dicts:
            s = Meters.create(
                serial_number = dic['Serial Number'], n_meter = dic['Meter No.'], panel_number = dic['Panel Number:'],
                job_number = dic['Job Number:'], job_name = dic['Job Name:'], seal = dic['SEAL:'], m_type = dic['Type:'],
                modbus_id = dic['MODBUS ID:']
            )
        s.save()
    except IntegrityError:
        print('\n')
        print('Entrada con el serial duplicado intente con un Serial distinto')
        print('\n')
        #raise ValueError('Entrada con el serial duplicado')

def view_entry(serial_number_tofind):
    """Hace una consulta a la base de datos por el numero de serial y lo muestra en pantalla"""
    
    entry = Meters.get(Meters.serial_number == serial_number_tofind)
    print('Serial Number:   {}'.format(entry.serial_number))
    print('Meter No.:   {}'.format(entry.n_meter))
    print('Panel Number:   {}'.format(entry.panel_number))
    print('Job Number:   {}'.format(entry.job_number))
    print('Job Name:   {}'.format(entry.job_name))
    print('Seal:   {}'.format(entry.seal))
    print('Meter type:   {}'.format(entry.m_type))
    print('Modbus is:   {}'.format(entry.modbus_id))
    print('Date:   {}'.format(entry.timestamp))


def get_dict_exceldata(path = 'data'):
    """Esta funcion extrae la información y la retorna en diccionarios"""

    os.chdir(path)
    files_excel = os.listdir()
    l_dic = []
    
    for file in files_excel:
        """Obtiene la información de los archivos de excel por DataFrames y se seccionan para facilitar la extracción de información"""

        df = pd.read_excel(file, skiprows = 1, names = range(0,13))
        first_sec = df.iloc[:5, lambda df: [0,3]]
        first_sec.columns = [0,1]

        secnd_sec = df.iloc[:5, lambda df: [6,9]]
        secnd_sec.columns = [0,1]
        secnd_sec[1] = secnd_sec[1].map({'X':True})

        trd_sec = df.iloc[25:31 , lambda df: [0,1,2]]
        trd_sec[1].fillna(trd_sec[2], inplace=True)
        trd_sec.drop(2, axis=1, inplace=True)

        ser_sec = df.iloc[46:70 , lambda df: [1,2]]
        ser_sec.columns = ser_sec.iloc[0]
        ser_sec.drop(46,axis=0,inplace = True)
        ser_sec.dropna(inplace = True)

        df_data = pd.concat([first_sec,secnd_sec,trd_sec]).dropna()
        df_data[1] = df_data[1].replace(['X'], [True])
        df_data[1] = df_data[1].replace([np.nan], [False])
        df_data = df_data.set_index(df_data[0]).drop(0, axis = 1)


        """ Se crea un diccionario por cada 'Serial Number' diferente"""
        for meter_ser in range(len(ser_sec)):
            dic_data = df_data.to_dict()[1]
            dic_data['Serial Number'] = ser_sec['Serial Number'].iloc[meter_ser]
            dic_data['Meter No.'] = ser_sec['Meter No.'].iloc[meter_ser]
            l_dic.append(dic_data)
   
    return l_dic
        

if __name__ == '__main__':
    
    initialize()

    #Extrayendo la informacion de los files de excel y agregandola a la base de datos
    data_files_dicts = get_dict_exceldata()
    add_entry(data_files_dicts)

    #Agregando una entrada manualmente
    
    dat_en = {
        'Serial Number': 'PCB-J-2018-22',
        'Meter No.': 1,
        'Panel Number:': '2DMP-2PP3BT',
        'Job Number:': '19-207-2954',
        'Job Name:': 'ECLRT - SCIENC CENTRE STATION',
        'SEAL:': True,
        'Type:': 'MF6',
        'MODBUS ID:': 3
    }
    entr = add_entry([dat_en])

    #Haciendo una consulta a la base de datos utilizando el 'Serial Number' como filtro
    view_entry(serial_number_tofind = 'PCB-J-2017-1')

    db.close()


