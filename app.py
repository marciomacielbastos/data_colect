import time
import datetime
from datetime import datetime, date, time
import json
# from airflow.api.client.local_client import Client
# from airflow.api.common.experimental import get_dag_runs
import streamlit as st
import pandas as pd
import altair as alt
import urllib
import logging
from psycopg2.sql import SQL, Identifier, Placeholder
from psycopg2 import connect, DatabaseError
from psycopg2.sql import SQL
import sys
# import delfos
# from delfos import MetaDAO
import numpy as np


preprocessors_flag0 = {'flag': 'missing_data',
                       'name': 'missing_data',
                       'params': {}},

preprocessors_filter = {'filter': 'KMeansDistanceDistribution',
                        'name': 'similarity',
                        'params': {'n_cluster': 12},
                        'use_in_test': False}

preprocessors_flag1 = {'flag': 'continuity', 'name': 'continuity', 'params': {'min_window': 6}}

threshold_params = {'kind': 'StatisticalThresholder',
                    'maximum': 0.99,
                    'minimum': None,
                    'distribution': 'norm'}

# class Client_delfos(Client):
#     def get_dag_runs(self, dag_id):
#         return get_dag_runs(dag_id)
    
#     def trigger(self, dag_id, run_id, conf={}):
#         if get_dag_runs(dag_id):
#             raise ConnectionAbortedError            
#         else:
#             self.trigger_dag(dag_id=dag_id,
#                              run_id=f"Training - Run: {pd.Timestamp('now')}",
#                              conf=conf)

# AIRFLOW_URL = 'http://airflow.delfos.im/'
# airflow_client = Client_delfos(AIRFLOW_URL, None)

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)

# def get_db_DAO(client_hash):
#     try:
#         meta_dao = MetaDAO()
#         meta_dao.DEBUG = False
#         return meta_dao.get_clientDAO_from_hash(client_hash) 
#     except Exception:
#         logging.error(f'Database {client_hash} doesnt exist')
#         return None

# def read_query(db_DAO, query, params=None):
#     db_DAO.DEBUG = False
#     data = db_DAO.read_query(query, params)
#     db_DAO.closeall()
#     return data

def get_dbHandler(conn_client):
    try:
        dhandler = DataHandler()
        dhandler.connect(conn_client)
        return dhandler

    except Exception:
        logging.error(f'Database doesnt exist')
        return None

def select(dbHandler, query, params=None):
    data = dbHandler.select(query, params)
    dbHandler.disconnect()
    return data

def insert(dbHandler, query, params=None):
    dbHandler.insert(query, params)
    dbHandler.disconnect()
    return

@st.cache
def get_all_variables_feat(scada):
    query = f"SELECT name, code, system_id FROM timeseries.variable WHERE scada_id = {scada.get_id()};"
    dbHandler = get_dbHandler(scada.get_client().get_conn())
    variables_table = select(dbHandler, query)
    return variables_table

@st.cache
def get_columns(client):
    query = "SELECT * FROM prediction.model LIMIT 0;"
    dbHandler = get_dbHandler(client.get_conn())
    cols = select(dbHandler, query)
    return list(cols.columns)[1:]

@st.cache
def get_all_oems(client):
    query = f"SELECT name FROM device.oem;"
    dbHandler = get_dbHandler(client.get_conn())
    oem_names = select(dbHandler, query)
    return oem_names

@st.cache
def get_scadas_of_the_client(client):
    query = "SELECT id, name FROM timeseries.scada;"
    dbHandler = get_dbHandler(client.get_conn())
    df = select(dbHandler, query)
    scada_dict = df.set_index("name").to_dict()['id']
    return scada_dict

@st.cache
def get_map_system_id_to_model_group_id(scada):
    query = "SELECT id AS system_id, model_group_id FROM device.system AS A INNER JOIN prediction.model_group AS B ON A.name ILIKE B.name;"
    dbHandler = get_dbHandler(scada.get_client().get_conn())
    df = select(dbHandler, query)
    map_stomg = dict(df.values)
    return map_stomg


class DataHandler:
    def __init__(self):
        self.conn_client = None
        self.conn = None
        
    def connect(self, conn_client=None):
        if conn_client is not None:
            self.conn_client = conn_client
        """ Connect to the PostgreSQL database server """
        self.disconnect()    
        try:
            # connect to the PostgreSQL server
            self.conn = connect(**self.conn_client)
        except (Exception, DatabaseError) as error:
            st.write(error)
            sys.exit(1)
    
    def disconnect(self):
        if self.conn is not None:
            print("Disconnecting")
            self.conn.close()
            self.conn = None
        
    def select(self, query, params=None):
        data =  pd.read_sql(sql=query, con=self.conn, params=params)
        return data

    def insert(self, query, params=None):
        cursor = self.conn.cursor()
        cursor.execute (query, params)
        self.conn.commit()
        
    def __del__(self):
        self.disconnect()

class ClientDB:
    def __init__(self):

        self.conn_client = {'host':'jericoacoara.delfos.im',
                            'port':"18494",
                            'dbname':"client00",
                            'user':"dim00",
                            'password':"password"}
        self.name = ""
        self.clients_dictionary = {"BVS": {'dbname':"client09",
                                           'user':"dim09",
                                           'password':"n0v0jo3lh0"},
                                   "ADS": {'dbname':"client13",
                                           'user':"dim13",
                                           'password':"seilamach0"},
                                   "Statkraft": {'dbname':"client25",
                                                 'user':"dim25",
                                                 'password':"starcraft2021"}}

    def select_client(self):  
        client = st.selectbox("Select client", 
                              [""] + list(self.clients_dictionary.keys()), 
                              format_func=lambda x: 'Select an option' if x == '' else x)
        self.name = client

    def set_hash(self, name):
        if name:
            self.conn_client.update(self.clients_dictionary[self.name])

    def get_name(self):
        return self.name
    
    def get_conn(self):
        return self.conn_client

class Scada:
    def __init__(self, client):     
        self.client = client
        self.scada_dict = get_scadas_of_the_client(client)
        self.name = ""
        self.oem = ""
        self.scada_id = None
        self.set_id()

    def select_name(self):
        scada_name = st.selectbox("Select scada", 
                     [""] + list(self.scada_dict.keys()), 
                     format_func=lambda x: 'Select an option' if x == '' else x)
        self.name = scada_name

    def set_id(self):
        self.scada_id = self.scada_dict.get(self.name, "")

    def select_oem(self):
        oem_names = list(get_all_oems(self.client)['name'].values)
        oem = st.selectbox("Select oem", [""] + oem_names, format_func=lambda x: 'Select an option' if x == '' else x)
        self.oem = oem

    def get_name(self):
        return self.name
    
    def get_id(self):
        return self.scada_id

    def get_client(self):
        return self.client

    def get_oem(self):
        return self.oem

class Variable:
    def __init__(self):
        self.code = ""
        self.name = ""
        self.alias = self.name
        self.min_value = 0
        self.max_value = 0

    def set_code(self, code):
        self.code = code

    def set_name(self, name):
        self.name = name

    def set_min_val(self):
        self.min_value = st.number_input(f'Minimun accepted value for {self.name}: ')

    def set_max_val(self):
        max_value = st.number_input(f'Maximun accepted value for {self.name}: ')
        if (self.min_value < max_value):
            self.max_value = max_value
        else:
            st.error("min value greater or equal than max value")

    def set_alias(self):
        alias = st.text_input(f'Set a alias for {self.name}: ')
        if alias:
            self.alias = alias
        else:
            self.alias = self.name

    def get_name(self):
        return self.name
    
    def get_code(self):
        return self.code

    def get_alias(self):
        return self.alias

    def get_min_val(self):
        return self.min_value

    def get_max_val(self):
        return self.max_value

    def to_dict(self):
        var_dict = {'max': self.max_value, 
                    'min': self.min_value,
                    'alias': self.alias,
                    'sql_expr': self.code}
        return var_dict

class Features:
    def __init__(self):
        self.variables = []
    
    def build_var(self, name_code_dict):
        var = Variable()
        var.set_name(name_code_dict["name"])
        var.set_code(f'var_{name_code_dict["code"]}')
        var.set_min_val()
        var.set_max_val()
        var.set_alias()
        return var

    def is_empty(self):
        return len(self.variables) == 0

class Inputs(Features):
    def select_variables(self, variables_name_code_table):
        st.subheader("Select input variabes")
        variable_names = list(variables_name_code_table['name'].values)
        variable_names.sort()
        variables_names = st.multiselect("Names", variable_names, [])
        name_code_table = variables_name_code_table[variables_name_code_table['name'].isin(variables_names)] 
        for name in variables_names:
            name_code_dict = name_code_table[name_code_table.name == name].to_dict('r')[0]
            st.write(name_code_dict)
            var = self.build_var(name_code_dict)
            self.variables.append(var)

    def to_dict(self,):
        variables = []
        for variable in self.variables:
            variables.append(variable.to_dict())
        return variables


class Output(Features):
    def __init__(self):
        super().__init__()
        self.system_id = -1

    def select_name(self, variables_name_code_group_table):
        outputs = list(variables_name_code_group_table['name'].values)
        outputs.sort()
        variable_name = st.selectbox("Name", [""] + outputs, format_func=lambda x: 'Select an option' if x == '' else x)
        return variable_name

    def select_variable(self, variables_name_code_group_table):
        st.subheader("Select output variabe")
        variable_name = self.select_name(variables_name_code_group_table)
        if not variable_name:
            st.warning("Choose one output option")
        else:
            name_code_sys_id_dict = variables_name_code_group_table[variables_name_code_group_table.name == variable_name].to_dict('r')[0]
            var = self.build_var(name_code_sys_id_dict)
            self.system_id = name_code_sys_id_dict['system_id']
            self.variables.append(var)

    def get_system_id(self):
        return self.system_id
    
    def get_alias(self):
        return self.variables[0].get_alias()

    def to_dict(self):
        if self.variables:
            variable = self.variables[0]
            variable_dict = variable.to_dict()
            variable_dict['group'] = self.system_id        
            return [variable.to_dict()]

class NeuralNetwork:
    def __init__(self):
        self.select_activation_function()
        self.set_epochs()
        self.set_validation_split()
        self.set_hidden_layers()
        self.set_cnn_filters()
        self.lstm_layers = self.set_lstm_layers('')
        self.bilstm_layers = self.set_lstm_layers('bi')
        self.output = ""

    def select_activation_function(self):
        activations = ['tanh', 'sigmoid', 'relu','elu']
        activation = st.selectbox("Activation", activations, format_func=lambda x: 'Select an option' if x == '' else x)
        if activation:
            self.activation = activation
        else:
            self.activation = 'tanh'

    def set_epochs(self):
        epochs = st.number_input('Epochs', value=250, format='%i')
        self.epochs = epochs

    def set_validation_split(self):
        split = st.number_input('Validation split', min_value=0.0, max_value=1.0, value=0.2)
        self.validation_split = split

    def set_hidden_layers(self):
        nlayers = st.number_input('# of layers', min_value=1, value=1, format='%i')
        layers = []
        for n in range(nlayers):
            layer = st.number_input(f'Size of layer {n}', min_value=1, value=30, format='%i')
            layers.append(layer)
        self.hidden_layers = layers  

    def set_cnn_filters(self):
        cnn_filters = st.number_input(f'CNN filters', min_value=0, value=0, format='%i')
        self.cnn_filters = cnn_filters

    def set_lstm_layers(self, lstm):
        nlayers = st.number_input(f'# of {lstm}lstm layers', min_value=0, value=0, format='%i')
        layers = []
        for n in range(nlayers):
            layer = st.number_input(f'Size of layer {n} ({lstm}lstm)', min_value=1, value=30, format='%i')
            layers.append(layer)
        return layers

    def set_output(self, output):
        self.output = output

    def get_activation_function(self):
        return self.activation

    def get_epochs(self):
        return self.epochs

    def get_validation_split(self):
        return self.validation_split

    def get_hidden_layers(self):
        return self.hidden_layers 

    def get_cnn_filters(self):
        return self.cnn_filters

    def get_lstm_layers(self):
        return self.lstm_layers

    def get_bilstm_layers(self):
        return self.bilstm_layers
    
    def to_dict(self):
        mlp_params = {'activation': self.activation,
                      'epochs': self.epochs,
                      'validation_split': self.validation_split,
                      'hidden_layer_sizes': self.hidden_layers,
                      'cnn_filters': self.cnn_filters,
                      'bilstm_layers': self.bilstm_layers,
                      'lstm_layers': self.lstm_layers}
        return mlp_params

class Model:
    def __init__(self, scada):
        self.name = ""
        self.description = "-2"
        self.scada = scada
        self.inputs = Inputs()
        self.output = Output()
        self.mlp = None
        self.regressor_params = {}
        self.date_set = {}
        self.variables_name_code_group_table = get_all_variables_feat(self.scada)
        self.map_stomg = get_map_system_id_to_model_group_id(self.scada)

    def set_inputs(self):
        self.inputs.select_variables(self.variables_name_code_group_table)

    def set_output(self):
        self.output.select_variable(self.variables_name_code_group_table)
    
    def variables_to_dict(self):
        if self.inputs.is_empty() and self.output.is_empty():
            return {}
        else:
            variables = {"inputs":self.inputs.to_dict(), "output":self.output.to_dict(), "sysid":self.output.get_system_id()}
            return variables

    def set_mlp(self):
        st.subheader("Setup mlp parameters")
        with st.beta_expander("Expand mlp parameters"):
            st.write("Here you can personilize your multilayer perceptron")
            mlp = NeuralNetwork()
        mlp.set_output(self.output.get_alias())
        self.mlp = mlp

    def wrap_mlp_on_dict(self):
        wrapped_mlp = {'kind': 'regression',
                       'params': self.mlp.to_dict(),
                       'prediction_model': 'MLPRegressorTF'}
        return wrapped_mlp

    def set_variables_flags(self):
        flags = {'flag': 'physical_limits',
                 'name': 'physical_limits',
                 'params': {'limits': self.inputs.to_dict() + self.output.to_dict()}}
        return flags
        
    def set_regressor_params(self):    
        params = {}
        params['model'] =  self.wrap_mlp_on_dict()
        params['model']['params']['output'] =  self.output.get_alias()     
        params['preprocessors'] = [preprocessors_flag0,
                                   self.set_variables_flags(),
                                   preprocessors_filter,
                                   preprocessors_flag1]
        params['timeseries_id'] = self.scada.get_id()
        self.regressor_params = params

    def set_time_interval(self, interval_name):
        st.subheader(f"{interval_name} interval")
        start_time = st.date_input(f'start {interval_name.lower()} date')
        end_time = st.date_input(f'end {interval_name.lower()} date')
        if (start_time >= end_time):
            st.error('start date must be before end date')
            return None, None
        else:
            start_time = start_time.strftime("%Y/%m/%d")
            end_time = end_time.strftime("%Y/%m/%d")
            return start_time, end_time

    def set_train_test_intervals(self):
        date_set = {}
        start_time_train, end_time_train = self.set_time_interval("Train")
        if start_time_train:
            start_time_test, end_time_test = self.set_time_interval("Test")
            if start_time_test:            
                date_set['start_time_train'] = start_time_train
                date_set['end_time_train'] = end_time_train
                date_set['start_time_test'] = start_time_test
                date_set['end_time_test'] = end_time_test
        self.date_set = date_set

    def does_name_exists(self, name):
        query = f"SELECT EXISTS (SELECT model_id FROM prediction.model WHERE name='{name}')::boolean;"
        dbHandler = get_dbHandler(self.scada.get_client().get_conn())
        is_unique = ~select(dbHandler, query)
        return is_unique.values[0]

    def set_name(self):
        name = st.text_input('Type a unique model name')
        if not name:
            st.warning("Type a unique model name")
        elif not self.does_name_exists(name):
            st.error("Model name already exists")
        else:
            self.name = name

    def set_description(self, description):
        description = st.text_input('Type a model description:')
        if description:
            self.description = description
        else:
            description = f"""Model framework usign Tensorflow to 
                              model {self.output.get_alias()}"""

    def get_name(self):
        return self.name

    def get_date_set(self):
        return self.date_set

    def to_dict(self):
        model_dict = {}
        model_dict.update(self.date_set)
        model_dict['threshold_params'] = threshold_params
        model_dict['description'] = self.description
        model_dict['model_group_id'] = int(self.map_stomg[self.output.get_system_id()])
        model_dict['module'] = 'framework'
        model_dict['device_manufacturer'] = self.scada.get_oem()
        model_dict['name'] = self.name
        model_dict['params'] = self.regressor_params
        return model_dict

def format_to_insert_tuple(model):
    try:
        insert_tuple = (1, model.get_name(), -2, -1, 
                        json.dumps(model.to_dict(), cls=NpEncoder), 
                        -1, False)
        return insert_tuple
    except TypeError as e :
        st.write("# TypeError", e)
        st.write("", model.to_dict())

def format_insert_parameters(scada, insert_tuple):
    params = {}
    cols = get_columns(scada.get_client())
    for pair in zip(cols, insert_tuple):
        params[pair[0]] = pair[1]
    return params

def format_insert_query(insert_parameters):
    keys = insert_parameters.keys()
    columns = SQL(',').join([Identifier(key) for key in keys])
    values = SQL(',').join([Placeholder(key) for key in keys])
    query = SQL("""
                    INSERT INTO prediction.model ({columns})
                    VALUES ({values})
                    RETURNING *;
                """).format(columns=columns,
                            values=values)
    return query

try:
    new_model = {}
    st.header("Setup")
    client = ClientDB()
    client.select_client()
    if not client.get_name():
        st.warning("Choose one client!")
    else:
        name = client.get_name()
        client.set_hash(name)
        scada = Scada(client)
        scada.select_oem()
        scada.select_name()
        scada.set_id()
        if not scada.get_oem():
            st.warning("Choose one oem!")       
        elif not scada.get_name():
            st.warning("Choose one scada name!")       
        else:
            model = Model(scada)
            model.set_name()
            if model.get_name():
                model.set_inputs()
                model.set_output()
                if model.variables_to_dict():         
                    model.set_train_test_intervals()                
                    if model.get_date_set():
                        model.set_mlp()
                        model.set_regressor_params()
                        if st.button("Show model's json"):
                            st.write("# Model: ", model.to_dict())
                        if st.button('Submit'):    
                            insert_tuple = format_to_insert_tuple(model)
                            insert_parameters = format_insert_parameters(scada, insert_tuple)
                            query = format_insert_query(insert_parameters)
                            insert(get_dbHandler(scada.get_client().get_conn()), query, params=insert_parameters)
                            st.write("Model sent")
                        # if st.button('Trigger'):
                        #     airflow_client.trigger(dag_id='airflow_mlflow_builder', run_id=f"airflow_mb Run: {pd.Timestamp('now')}")
                        

except urllib.error.URLError as e:
    st.error(
        """
        **This demo requires internet access.**

        Connection error: %s
    """
        % e.reason
    )