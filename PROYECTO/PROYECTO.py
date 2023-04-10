#!/usr/bin/env python
# coding: utf-8

# In[ ]:


##### Nombre: Pablo César Méndez Rodas
##### Carnet: 22006487
##### Ciencia de Datos en Python, sección B
##### PAPD - Primer trimestre 2023


# In[ ]:


# PROYECTO FINAL


# In[4]:


import pandas as pd
import numpy as np
from faker import Faker
import random
import datetime
import boto3
import psycopg2
import configparser


# ## SCOPE

# ##### Para estre proyecto se eligió la base de datos relacional de unas tiendas que rentan peliculas la cual tiene un total de 15 tablas pensadas para un sistema transaccional.

# In[ ]:


from IPython.display import Image
Image(filename='db oltp.png')


# ## EXPLORACIÓN Y MODELO DE DATOS

# ##### Durante la exploracion de datos se ha decidido que se trabajará un Data Warehouse para poder responder preguntas de analítica útiles para las deciciones del negocio. La base de datos estará cargada en AWS así como tambien el Data Warehouse.

# ## PROCESAMIENTO

# ### BASE DE DATOS RELACIONAL

# #### Cargamos archivo de configuraciones

# In[11]:


rdsIdentifier = 'rental-dbpro' #nombre de la instancia
config = configparser.ConfigParser()
config.read('escec.cfg')


# ### Creamos Instancia de RDS 

# In[6]:


aws_conn = boto3.client('rds', aws_access_key_id=config.get('IAM', 'ACCESS_KEY'),
                    aws_secret_access_key=config.get('IAM', 'SECRET_ACCESS_KEY'),
                    region_name='us-east-1')


# #### Verificamos Instancias de RDS disponibles

# In[7]:


rdsInstanceIds = []

response = aws_conn.describe_db_instances()
for resp in response['DBInstances']:
    rdsInstanceIds.append(resp['DBInstanceIdentifier'])
    db_instance_status = resp['DBInstanceStatus']

print(f"DBInstanceIds {rdsInstanceIds}")


# #### Creación de Servicio RDS

# In[65]:


try:
    response = aws_conn.create_db_instance(
            AllocatedStorage=10,
            DBName=config.get('RDS_OLTP', 'DB_NAME'),
            DBInstanceIdentifier=rdsIdentifier,
            DBInstanceClass="db.t3.micro",
            Engine="postgres",
            MasterUsername=config.get('RDS_OLTP', 'DB_USER'),
            MasterUserPassword=config.get('RDS_OLTP', 'DB_PASSWORD'),
            Port=int(config.get('RDS_OLTP', 'DB_PORT')),
            VpcSecurityGroupIds=[config.get('VPC', 'SECURITY_GROUP')],
            PubliclyAccessible=True
        )
    print(response)
except aws_conn.exceptions.DBInstanceAlreadyExistsFault as ex:
    print("La Instancia de Base de Datos ya Existe.")


# ##### Recordemos Esperar unos minutos para consultar la informaicón de la instancia.

# ##### Obtenemos URL del Host

# In[31]:


try:
     instances = aws_conn.describe_db_instances(DBInstanceIdentifier=rdsIdentifier)
     RDS_HOST = instances.get('DBInstances')[0].get('Endpoint').get('Address')
     print(RDS_HOST)
except Exception as ex:
     print("La instancia de base de datos no existe o aun no se ha terminado de crear.")
     print(ex)


# ##### Conexión a Base de Datos desde Python

# In[93]:


import sql_db7 #archivo externo que contiene el query para crear la base de datos relacional


# In[94]:


try:
    db_conn = psycopg2.connect(
        database=config.get('RDS_OLTP', 'DB_NAME'), 
        user=config.get('RDS_OLTP', 'DB_USER'),
        password=config.get('RDS_OLTP', 'DB_PASSWORD'), 
        host=RDS_HOST,
        port=config.get('RDS_OLTP', 'DB_PORT')
    )

    cursor = db_conn.cursor()
    cursor.execute(sql_db7.DDL_QUERY)
    db_conn.commit()
    print("Base de Datos Creada Exitosamente")
except Exception as ex:
    print("ERROR: Error al crear la base de datos.")
    print(ex)


# #### Conexion hacia S3 en AWS

# In[101]:


s3 = boto3.resource(
    service_name = 's3',
    region_name = 'us-east-1',
    aws_access_key_id = config.get('IAM', 'ACCESS_KEY'),
    aws_secret_access_key = config.get('IAM', 'SECRET_ACCESS_KEY')
)


# #### Obtener los nombres de cada bucket en S3

# In[ ]:


for bucket in s3.buckets.all():
    S3_BUCKET_NAME = bucket.name
    print(bucket.name)


# #### Asignar variable con el bucket que se usara

# In[34]:


S3_BUCKET_NAME = 'proyectorental'


# #### Extraermos lista de archivos en el bucket

# In[39]:


#extraemos todo lo que está en el bucket
remoteFileList = []
for objt in s3.Bucket(S3_BUCKET_NAME).objects.all():
    remoteFileList.append(objt.key)

remoteFileList.sort()
remoteFileList


# #### Obtenemos lista de las tablas creadas en la base de datos

# In[40]:


db_conn = psycopg2.connect(
    database=config.get('RDS_OLTP', 'DB_NAME'), 
    user=config.get('RDS_OLTP', 'DB_USER'),
    password=config.get('RDS_OLTP', 'DB_PASSWORD'), 
    host=RDS_HOST,
    port=config.get('RDS_OLTP', 'DB_PORT')
)

cursor = db_conn.cursor()
cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'")
db_conn.commit()
# Fetch all the rows and print the table names
rows = cursor.fetchall()
remoteTableList = []
for row in rows:
    remoteTableList.append(row[0])

remoteTableList.sort()
print(remoteTableList)
    
# Close the cursor and the connection
cursor.close()
db_conn.close()


# #### Confirmamos que cada tabla tenga su archivo cargado en S3

# In[41]:


for n in range(0,len(remoteTableList)):
    print(remoteTableList[n])
    print(remoteFileList[n])


# #### Llenamos cada tabla de la base de datos con los archivos cargados en S3

# In[ ]:


# Connect to S3
s3 = boto3.resource(
    service_name = 's3',
    region_name = 'us-east-1',
    aws_access_key_id = config.get('IAM', 'ACCESS_KEY'),
    aws_secret_access_key = config.get('IAM', 'SECRET_ACCESS_KEY')
)


bucket = s3.Bucket(S3_BUCKET_NAME)
obj = bucket.Object('actor.dat')

    # Connect to PostgreSQL
tab_conn = psycopg2.connect(
        host=RDS_HOST,
        database=config.get('RDS_OLTP', 'DB_NAME'),
        user=config.get('RDS_OLTP', 'DB_USER'),
        password=config.get('RDS_OLTP', 'DB_PASSWORD'),
    )

    # Open a cursor to perform database operations
cur = tab_conn.cursor()

    # Copy the data from S3 to PostgreSQL
cur.copy_from(obj.get()['Body'], 'actor', sep='\t')

    # Commit the transaction and close the cursor and connection
tab_conn.commit()
cur.close()
tab_conn.close()


# In[162]:


# Connect to S3
s3 = boto3.resource(
    service_name = 's3',
    region_name = 'us-east-1',
    aws_access_key_id = config.get('IAM', 'ACCESS_KEY'),
    aws_secret_access_key = config.get('IAM', 'SECRET_ACCESS_KEY')
)


bucket = s3.Bucket(S3_BUCKET_NAME)
obj = bucket.Object('country.dat')

    # Connect to PostgreSQL
tab_conn = psycopg2.connect(
        host=RDS_HOST,
        database=config.get('RDS_OLTP', 'DB_NAME'),
        user=config.get('RDS_OLTP', 'DB_USER'),
        password=config.get('RDS_OLTP', 'DB_PASSWORD'),
    )

    # Open a cursor to perform database operations
cur = tab_conn.cursor()

    # Copy the data from S3 to PostgreSQL
cur.copy_from(obj.get()['Body'], 'country', sep='\t')

    # Commit the transaction and close the cursor and connection
tab_conn.commit()
cur.close()
tab_conn.close()


# In[163]:


# Connect to S3
s3 = boto3.resource(
    service_name = 's3',
    region_name = 'us-east-1',
    aws_access_key_id = config.get('IAM', 'ACCESS_KEY'),
    aws_secret_access_key = config.get('IAM', 'SECRET_ACCESS_KEY')
)


bucket = s3.Bucket(S3_BUCKET_NAME)
obj = bucket.Object('city.dat')

    # Connect to PostgreSQL
tab_conn = psycopg2.connect(
        host=RDS_HOST,
        database=config.get('RDS_OLTP', 'DB_NAME'),
        user=config.get('RDS_OLTP', 'DB_USER'),
        password=config.get('RDS_OLTP', 'DB_PASSWORD'),
    )

    # Open a cursor to perform database operations
cur = tab_conn.cursor()

    # Copy the data from S3 to PostgreSQL
cur.copy_from(obj.get()['Body'], 'city', sep='\t')

    # Commit the transaction and close the cursor and connection
tab_conn.commit()
cur.close()
tab_conn.close()


# In[164]:


# Connect to S3
s3 = boto3.resource(
    service_name = 's3',
    region_name = 'us-east-1',
    aws_access_key_id = config.get('IAM', 'ACCESS_KEY'),
    aws_secret_access_key = config.get('IAM', 'SECRET_ACCESS_KEY')
)


bucket = s3.Bucket(S3_BUCKET_NAME)
obj = bucket.Object('address.dat')

    # Connect to PostgreSQL
tab_conn = psycopg2.connect(
        host=RDS_HOST,
        database=config.get('RDS_OLTP', 'DB_NAME'),
        user=config.get('RDS_OLTP', 'DB_USER'),
        password=config.get('RDS_OLTP', 'DB_PASSWORD'),
    )

    # Open a cursor to perform database operations
cur = tab_conn.cursor()

    # Copy the data from S3 to PostgreSQL
cur.copy_from(obj.get()['Body'], 'address', sep='\t')

    # Commit the transaction and close the cursor and connection
tab_conn.commit()
cur.close()
tab_conn.close()


# In[166]:


# Connect to S3
s3 = boto3.resource(
    service_name = 's3',
    region_name = 'us-east-1',
    aws_access_key_id = config.get('IAM', 'ACCESS_KEY'),
    aws_secret_access_key = config.get('IAM', 'SECRET_ACCESS_KEY')
)


bucket = s3.Bucket(S3_BUCKET_NAME)
obj = bucket.Object('category.dat')

    # Connect to PostgreSQL
tab_conn = psycopg2.connect(
        host=RDS_HOST,
        database=config.get('RDS_OLTP', 'DB_NAME'),
        user=config.get('RDS_OLTP', 'DB_USER'),
        password=config.get('RDS_OLTP', 'DB_PASSWORD'),
    )

    # Open a cursor to perform database operations
cur = tab_conn.cursor()

    # Copy the data from S3 to PostgreSQL
cur.copy_from(obj.get()['Body'], 'category', sep='\t')

    # Commit the transaction and close the cursor and connection
tab_conn.commit()
cur.close()
tab_conn.close()


# In[167]:


# Connect to S3
s3 = boto3.resource(
    service_name = 's3',
    region_name = 'us-east-1',
    aws_access_key_id = config.get('IAM', 'ACCESS_KEY'),
    aws_secret_access_key = config.get('IAM', 'SECRET_ACCESS_KEY')
)


bucket = s3.Bucket(S3_BUCKET_NAME)
obj = bucket.Object('customer.dat')

    # Connect to PostgreSQL
tab_conn = psycopg2.connect(
        host=RDS_HOST,
        database=config.get('RDS_OLTP', 'DB_NAME'),
        user=config.get('RDS_OLTP', 'DB_USER'),
        password=config.get('RDS_OLTP', 'DB_PASSWORD'),
    )

    # Open a cursor to perform database operations
cur = tab_conn.cursor()

    # Copy the data from S3 to PostgreSQL
cur.copy_from(obj.get()['Body'], 'customer', sep='\t')

    # Commit the transaction and close the cursor and connection
tab_conn.commit()
cur.close()
tab_conn.close()


# In[169]:


# Connect to S3
s3 = boto3.resource(
    service_name = 's3',
    region_name = 'us-east-1',
    aws_access_key_id = config.get('IAM', 'ACCESS_KEY'),
    aws_secret_access_key = config.get('IAM', 'SECRET_ACCESS_KEY')
)


bucket = s3.Bucket(S3_BUCKET_NAME)
obj = bucket.Object('language.dat')

    # Connect to PostgreSQL
tab_conn = psycopg2.connect(
        host=RDS_HOST,
        database=config.get('RDS_OLTP', 'DB_NAME'),
        user=config.get('RDS_OLTP', 'DB_USER'),
        password=config.get('RDS_OLTP', 'DB_PASSWORD'),
    )

    # Open a cursor to perform database operations
cur = tab_conn.cursor()

    # Copy the data from S3 to PostgreSQL
cur.copy_from(obj.get()['Body'], 'language', sep='\t')

    # Commit the transaction and close the cursor and connection
tab_conn.commit()
cur.close()
tab_conn.close()


# In[170]:


# Connect to S3
s3 = boto3.resource(
    service_name = 's3',
    region_name = 'us-east-1',
    aws_access_key_id = config.get('IAM', 'ACCESS_KEY'),
    aws_secret_access_key = config.get('IAM', 'SECRET_ACCESS_KEY')
)


bucket = s3.Bucket(S3_BUCKET_NAME)
obj = bucket.Object('film.dat')

    # Connect to PostgreSQL
tab_conn = psycopg2.connect(
        host=RDS_HOST,
        database=config.get('RDS_OLTP', 'DB_NAME'),
        user=config.get('RDS_OLTP', 'DB_USER'),
        password=config.get('RDS_OLTP', 'DB_PASSWORD'),
    )

    # Open a cursor to perform database operations
cur = tab_conn.cursor()

    # Copy the data from S3 to PostgreSQL
cur.copy_from(obj.get()['Body'], 'film', sep='\t')

    # Commit the transaction and close the cursor and connection
tab_conn.commit()
cur.close()
tab_conn.close()


# In[171]:


# Connect to S3
s3 = boto3.resource(
    service_name = 's3',
    region_name = 'us-east-1',
    aws_access_key_id = config.get('IAM', 'ACCESS_KEY'),
    aws_secret_access_key = config.get('IAM', 'SECRET_ACCESS_KEY')
)


bucket = s3.Bucket(S3_BUCKET_NAME)
obj = bucket.Object('film_actor.dat')

    # Connect to PostgreSQL
tab_conn = psycopg2.connect(
        host=RDS_HOST,
        database=config.get('RDS_OLTP', 'DB_NAME'),
        user=config.get('RDS_OLTP', 'DB_USER'),
        password=config.get('RDS_OLTP', 'DB_PASSWORD'),
    )

    # Open a cursor to perform database operations
cur = tab_conn.cursor()

    # Copy the data from S3 to PostgreSQL
cur.copy_from(obj.get()['Body'], 'film_actor', sep='\t')

    # Commit the transaction and close the cursor and connection
tab_conn.commit()
cur.close()
tab_conn.close()


# In[172]:


# Connect to S3
s3 = boto3.resource(
    service_name = 's3',
    region_name = 'us-east-1',
    aws_access_key_id = config.get('IAM', 'ACCESS_KEY'),
    aws_secret_access_key = config.get('IAM', 'SECRET_ACCESS_KEY')
)


bucket = s3.Bucket(S3_BUCKET_NAME)
obj = bucket.Object('film_category.dat')

    # Connect to PostgreSQL
tab_conn = psycopg2.connect(
        host=RDS_HOST,
        database=config.get('RDS_OLTP', 'DB_NAME'),
        user=config.get('RDS_OLTP', 'DB_USER'),
        password=config.get('RDS_OLTP', 'DB_PASSWORD'),
    )

    # Open a cursor to perform database operations
cur = tab_conn.cursor()

    # Copy the data from S3 to PostgreSQL
cur.copy_from(obj.get()['Body'], 'film_category', sep='\t')

    # Commit the transaction and close the cursor and connection
tab_conn.commit()
cur.close()
tab_conn.close()


# In[173]:


# Connect to S3
s3 = boto3.resource(
    service_name = 's3',
    region_name = 'us-east-1',
    aws_access_key_id = config.get('IAM', 'ACCESS_KEY'),
    aws_secret_access_key = config.get('IAM', 'SECRET_ACCESS_KEY')
)


bucket = s3.Bucket(S3_BUCKET_NAME)
obj = bucket.Object('inventory.dat')

    # Connect to PostgreSQL
tab_conn = psycopg2.connect(
        host=RDS_HOST,
        database=config.get('RDS_OLTP', 'DB_NAME'),
        user=config.get('RDS_OLTP', 'DB_USER'),
        password=config.get('RDS_OLTP', 'DB_PASSWORD'),
    )

    # Open a cursor to perform database operations
cur = tab_conn.cursor()

    # Copy the data from S3 to PostgreSQL
cur.copy_from(obj.get()['Body'], 'inventory', sep='\t')

    # Commit the transaction and close the cursor and connection
tab_conn.commit()
cur.close()
tab_conn.close()


# In[176]:


# Connect to S3
s3 = boto3.resource(
    service_name = 's3',
    region_name = 'us-east-1',
    aws_access_key_id = config.get('IAM', 'ACCESS_KEY'),
    aws_secret_access_key = config.get('IAM', 'SECRET_ACCESS_KEY')
)


bucket = s3.Bucket(S3_BUCKET_NAME)
obj = bucket.Object('staff.dat')

    # Connect to PostgreSQL
tab_conn = psycopg2.connect(
        host=RDS_HOST,
        database=config.get('RDS_OLTP', 'DB_NAME'),
        user=config.get('RDS_OLTP', 'DB_USER'),
        password=config.get('RDS_OLTP', 'DB_PASSWORD'),
    )

    # Open a cursor to perform database operations
cur = tab_conn.cursor()

    # Copy the data from S3 to PostgreSQL
cur.copy_from(obj.get()['Body'], 'staff', sep='\t')

    # Commit the transaction and close the cursor and connection
tab_conn.commit()
cur.close()
tab_conn.close()


# In[177]:


# Connect to S3
s3 = boto3.resource(
    service_name = 's3',
    region_name = 'us-east-1',
    aws_access_key_id = config.get('IAM', 'ACCESS_KEY'),
    aws_secret_access_key = config.get('IAM', 'SECRET_ACCESS_KEY')
)


bucket = s3.Bucket(S3_BUCKET_NAME)
obj = bucket.Object('rental.dat')

    # Connect to PostgreSQL
tab_conn = psycopg2.connect(
        host=RDS_HOST,
        database=config.get('RDS_OLTP', 'DB_NAME'),
        user=config.get('RDS_OLTP', 'DB_USER'),
        password=config.get('RDS_OLTP', 'DB_PASSWORD'),
    )

    # Open a cursor to perform database operations
cur = tab_conn.cursor()

    # Copy the data from S3 to PostgreSQL
cur.copy_from(obj.get()['Body'], 'rental', sep='\t')

    # Commit the transaction and close the cursor and connection
tab_conn.commit()
cur.close()
tab_conn.close()


# In[178]:


# Connect to S3
s3 = boto3.resource(
    service_name = 's3',
    region_name = 'us-east-1',
    aws_access_key_id = config.get('IAM', 'ACCESS_KEY'),
    aws_secret_access_key = config.get('IAM', 'SECRET_ACCESS_KEY')
)


bucket = s3.Bucket(S3_BUCKET_NAME)
obj = bucket.Object('payment.dat')

    # Connect to PostgreSQL
tab_conn = psycopg2.connect(
        host=RDS_HOST,
        database=config.get('RDS_OLTP', 'DB_NAME'),
        user=config.get('RDS_OLTP', 'DB_USER'),
        password=config.get('RDS_OLTP', 'DB_PASSWORD'),
    )

    # Open a cursor to perform database operations
cur = tab_conn.cursor()

    # Copy the data from S3 to PostgreSQL
cur.copy_from(obj.get()['Body'], 'payment', sep='\t')

    # Commit the transaction and close the cursor and connection
tab_conn.commit()
cur.close()
tab_conn.close()


# In[179]:


# Connect to S3
s3 = boto3.resource(
    service_name = 's3',
    region_name = 'us-east-1',
    aws_access_key_id = config.get('IAM', 'ACCESS_KEY'),
    aws_secret_access_key = config.get('IAM', 'SECRET_ACCESS_KEY')
)


bucket = s3.Bucket(S3_BUCKET_NAME)
obj = bucket.Object('store.dat')

    # Connect to PostgreSQL
tab_conn = psycopg2.connect(
        host=RDS_HOST,
        database=config.get('RDS_OLTP', 'DB_NAME'),
        user=config.get('RDS_OLTP', 'DB_USER'),
        password=config.get('RDS_OLTP', 'DB_PASSWORD'),
    )

    # Open a cursor to perform database operations
cur = tab_conn.cursor()

    # Copy the data from S3 to PostgreSQL
cur.copy_from(obj.get()['Body'], 'store', sep='\t')

    # Commit the transaction and close the cursor and connection
tab_conn.commit()
cur.close()
tab_conn.close()


# #### Asignamos variable para conectar a la base de datos

# In[32]:


postgres_driver = f"""postgresql://{config.get('RDS_OLTP', 'DB_USER')}:{config.get('RDS_OLTP', 'DB_PASSWORD')}@{RDS_HOST}:{config.get('RDS_OLTP', 'DB_PORT')}/{config.get('RDS_OLTP', 'DB_NAME')}"""  


# #### Verificamos que los datos se hayan cargado a cada tabla

# In[42]:


for n in range(0,len(remoteTableList)):
    sql_query = f'SELECT * FROM {remoteTableList[n]};'
    conteo = pd.read_sql(sql_query, postgres_driver)
    print(remoteTableList[n],len(conteo))


# #### Cargamos todas las tablas en dataframes de python

# In[43]:


for i in remoteTableList:
    df_name= f'df_{i}'
    sql_query = f'SELECT * FROM {i}'
    df = pd.read_sql(sql_query, postgres_driver)
    exec(f"{df_name} = df")
 


# #### Confirmamos que se hayan creado bien los DataFrames de Pandas

# In[132]:


df_customer.head()


# ### DATA WAREHOUSE

# #### Creando dimension Customers

# In[44]:


df_customer['customer_name']=df_customer['first_name']+" "+df_customer['last_name']
df_custo_addr = df_customer.merge(df_address, on='address_id', how='inner')
df_custo_addr_city = df_custo_addr.merge(df_city, on='city_id', how='inner')

dimCustomer = df_custo_addr_city.merge(df_country, on='country_id', how='inner')
dimCustomer.rename(columns = {'address':'customer_address', 'district':'customer_district','city':'customer_city','country':'customer_country'}, inplace = True)
col_keep = ['customer_id','customer_name', 'email','customer_address', 'customer_district', 'customer_city', 'customer_country', 'activebool']
dimCustomer = dimCustomer.loc[:,col_keep]
dimCustomer.head()


# #### Creando la dimension Film

# In[78]:


df_actor['actor']=df_actor['first_name']+" "+df_actor['last_name']
df_category.rename(columns = {'name':'category'}, inplace = True)
df_language.rename(columns = {'name':'language'}, inplace = True)

df_film_lan = df_film.merge(df_language, on='language_id', how='inner',suffixes=('', '_l'))

df_film_lan_factor = df_film_lan.merge(df_film_actor, on='film_id', how='inner',suffixes=('', '_fa'))

df_film_lan_factor_act = df_film_lan_factor.merge(df_actor, on='actor_id', how='inner',suffixes=('', '_a'))

df_film_lan_factor_act_fcat = df_film_lan_factor_act.merge(df_film_category, on='film_id', how='inner',suffixes=('', '_fc'))

dimFilm = df_film_lan_factor_act_fcat.merge(df_category, on='category_id', how='inner',suffixes=('', '_a'))

col_keep = ['film_id','title','actor','category','release_year','language','length','replacement_cost','rating']

dimFilm = dimFilm.loc[:,col_keep]
dimFilm.drop_duplicates(subset=['film_id'], inplace=True)

dimFilm.head()


# #### Creando dimension Store

# In[81]:


df_staff['staff_name']=df_staff['first_name']+" "+df_staff['last_name']
df_staff_store = df_staff.merge(df_store, on='store_id', how='inner',suffixes=('', '_s'))
df_sta_sto_addr = df_staff_store.merge(df_address, on='address_id', how='inner',suffixes=('', '_a'))
df_sta_sto_addr_cit = df_sta_sto_addr.merge(df_city, on='city_id', how='inner',suffixes=('', '_c'))
dimStore = df_sta_sto_addr_cit.merge(df_country, on='country_id', how='inner',suffixes=('', '_co'))
dimStore.rename(columns = {'address':'store_address', 'district':'store_district','city':'store_city','country':'store_country'}, inplace = True)
col_keep = ['staff_id','staff_name','store_id','store_address','store_district','store_city','store_country']
dimStore = dimStore.loc[:,col_keep]
dimStore.drop_duplicates(subset=['staff_id'], inplace=True)
dimStore.head()


# #### Creando dimension Dates

# In[85]:


all_dates = df_payment.merge(df_rental, on='rental_id', how='inner',suffixes=('', '_d'))
all_dates.head()
dimDate = pd.concat([all_dates['payment_date'],all_dates['rental_date'],all_dates['return_date']])
dimDate = dimDate.dropna().drop_duplicates()
dimDate = pd.DataFrame({'date_time':dimDate})

dimDate['year'] = pd.DatetimeIndex(dimDate['date_time']).year
dimDate['month'] = pd.DatetimeIndex(dimDate['date_time']).month
dimDate['day'] = pd.DatetimeIndex(dimDate['date_time']).day
dimDate['dayofweek'] = pd.DatetimeIndex(dimDate['date_time']).dayofweek
dimDate['hour'] = pd.DatetimeIndex(dimDate['date_time']).hour
dimDate['minute'] = pd.DatetimeIndex(dimDate['date_time']).minute

dimDate['date_id'] = dimDate['year'].astype(str) + dimDate['month'].astype(str)
dimDate['date_id'] = dimDate['date_id'].astype(str) + dimDate['day'].astype(str)
dimDate['date_id'] = dimDate['date_id'].astype(str) + dimDate['hour'].astype(str)
dimDate['date_id'] = dimDate['date_id'].astype(str) + dimDate['minute'].astype(str)
dimDate.drop_duplicates(subset=['date_id'],inplace=True)
dimDate.head()

col_keep = ['date_id','year','month','day','dayofweek','hour', 'minute', 'date_time']
dimDate = dimDate.loc[:,col_keep]
dimDate.head()


# #### Creando fact table

# In[91]:


df_pay_rent = df_payment.merge(df_rental, on='rental_id', how='inner',suffixes=('', '_r'))
factRental = df_pay_rent.merge(df_inventory, on='inventory_id', how='inner',suffixes=('', '_i'))

factRental['year'] = pd.DatetimeIndex(factRental['payment_date']).year
factRental['month'] = pd.DatetimeIndex(factRental['payment_date']).month
factRental['day'] = pd.DatetimeIndex(factRental['payment_date']).day
factRental['dayofweek'] = pd.DatetimeIndex(factRental['payment_date']).dayofweek
factRental['hour'] = pd.DatetimeIndex(factRental['payment_date']).hour
factRental['minute'] = pd.DatetimeIndex(factRental['payment_date']).minute

factRental['date_id'] = factRental['year'].astype(str) + factRental['month'].astype(str)
factRental['date_id'] = factRental['date_id'].astype(str) + factRental['day'].astype(str)
factRental['date_id'] = factRental['date_id'].astype(str) + factRental['hour'].astype(str)
factRental['date_id'] = factRental['date_id'].astype(str) + factRental['minute'].astype(str)


col_keep = ['payment_id','customer_id','staff_id','film_id','date_id','amount','payment_date','rental_date','return_date']
factRental = factRental.loc[:,col_keep]
factRental = factRental[factRental['film_id'] != 257]
factRental = factRental[factRental['film_id'] != 323]
factRental.head()


# In[8]:


rdsIdentifierDW = 'rental-dwpro' #nombre de la instancia


# In[13]:


rdsInstanceIds = []

response = aws_conn.describe_db_instances()
for resp in response['DBInstances']:
    rdsInstanceIds.append(resp['DBInstanceIdentifier'])
    db_instance_status = resp['DBInstanceStatus']

print(f"DBInstanceIds {rdsInstanceIds}")


# In[12]:


try:
    response = aws_conn.create_db_instance(
            AllocatedStorage=10,
            DBName=config.get('RDS_DW', 'DB_NAME'),
            DBInstanceIdentifier=rdsIdentifierDW,
            DBInstanceClass="db.t3.micro",
            Engine="postgres",
            MasterUsername=config.get('RDS_DW', 'DB_USER'),
            MasterUserPassword=config.get('RDS_DW', 'DB_PASSWORD'),
            Port=int(config.get('RDS_DW', 'DB_PORT')),
            VpcSecurityGroupIds=[config.get('VPC', 'SECURITY_GROUP')],
            PubliclyAccessible=True
        )
    print(response)
except aws_conn.exceptions.DBInstanceAlreadyExistsFault as ex:
    print("La Instancia de Base de Datos ya Existe.")


# In[14]:


try:
     instances = aws_conn.describe_db_instances(DBInstanceIdentifier=rdsIdentifierDW)
     RDS_HOST_DW = instances.get('DBInstances')[0].get('Endpoint').get('Address')
     print(RDS_HOST_DW)
except Exception as ex:
     print("La instancia de base de datos no existe o aun no se ha terminado de crear.")
     print(ex)


# In[22]:


import sql_dw2


# In[23]:


try:
    dw_conn = psycopg2.connect(
        database=config.get('RDS_DW', 'DB_NAME'), 
        user=config.get('RDS_DW', 'DB_USER'),
        password=config.get('RDS_DW', 'DB_PASSWORD'), 
        host=RDS_HOST_DW,
        port=config.get('RDS_DW', 'DB_PORT')
    )

    cursor = dw_conn.cursor()
    cursor.execute(sql_dw2.DDL_QUERY_DW)
    dw_conn.commit()
    print("Base de Datos Creada Exitosamente")
except Exception as ex:
    print("ERROR: Error al crear la base de datos.")
    print(ex)


# In[53]:


postgres_driver_dw = f"""postgresql://{config.get('RDS_DW', 'DB_USER')}:{config.get('RDS_DW', 'DB_PASSWORD')}@{RDS_HOST_DW}:{config.get('RDS_DW', 'DB_PORT')}/{config.get('RDS_DW', 'DB_NAME')}"""  


# In[63]:


#insertamos customers.
dimCustomer.to_sql('dimcustomer', postgres_driver_dw, index=False, if_exists='append')


# In[79]:


#insertamos film
dimFilm.to_sql('dimfilm', postgres_driver_dw, index=False, if_exists='append')


# In[82]:


#insertamos store
dimStore.to_sql('dimstore', postgres_driver_dw, index=False, if_exists='append')


# In[86]:


#insertamos date
dimDate.to_sql('dimdate', postgres_driver_dw, index=False, if_exists='append')


# In[97]:


#insertamos fact
factRental.to_sql('factrental', postgres_driver_dw, index=False, if_exists='append')


# In[59]:


dw_conn = psycopg2.connect(
    database=config.get('RDS_DW', 'DB_NAME'), 
    user=config.get('RDS_DW', 'DB_USER'),
    password=config.get('RDS_DW', 'DB_PASSWORD'), 
    host=RDS_HOST_DW,
    port=config.get('RDS_DW', 'DB_PORT')
)

cursor = dw_conn.cursor()
cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'")
dw_conn.commit()
# Fetch all the rows and print the table names
rows = cursor.fetchall()
remoteTableList = []
for row in rows:
    remoteTableList.append(row[0])

remoteTableList.sort()
print(remoteTableList)
    
# Close the cursor and the connection
cursor.close()
dw_conn.close()


# In[99]:


try:
    for n in range(0,len(remoteTableList)):
        sql_query = f'SELECT * FROM {remoteTableList[n]};'
        conteo = pd.read_sql(sql_query, postgres_driver_dw)
        print(remoteTableList[n],len(conteo))
    print("Data Warehouse cargado correctamente")
except:
    print("Algo salió mal")


# In[ ]:


Image(filename='dw.png')


# # Preguntas de analitica  

# In[124]:


#Obteniendo las categorias de peliculas mas rentadas

sql_query = '''SELECT dimfilm.category, COUNT(dimfilm.category) as rental_count
                FROM dimfilm 
                JOIN factrental ON factrental.film_id = dimfilm.film_id
                GROUP BY dimfilm.category
                ORDER BY rental_count DESC;'''
pd.read_sql(sql_query, postgres_driver_dw)


# In[123]:


#Clientes que no devuelven peliculas

sql_query = '''SELECT d.customer_id, d.customer_name, f.return_date  
                FROM factrental f
                JOIN dimcustomer d ON d.customer_id = f.customer_id
                WHERE f.return_date IS NULL;'''
pd.read_sql(sql_query, postgres_driver_dw)


# In[126]:


#Paises donde rentan mas peliculas

sql_query = '''SELECT dimcustomer.customer_country, COUNT(dimcustomer.customer_country) as rental_count
                FROM dimcustomer 
                JOIN factrental ON factrental.customer_id = dimcustomer.customer_id
                GROUP BY dimcustomer.customer_country
                ORDER BY rental_count DESC;'''
pd.read_sql(sql_query, postgres_driver_dw)


# In[128]:


#Promedio de dias que tarda la renta

sql_query = '''SELECT avg(DATE_PART('day',return_date  - rental_date))  AS days_diff
                FROM factrental
                WHERE return_date IS NOT NULL;'''
pd.read_sql(sql_query, postgres_driver_dw)



# In[130]:


#Suma de las ventas totales

sql_query = '''SELECT SUM(amount) as total_sales FROM factrental f  ;'''
pd.read_sql(sql_query, postgres_driver_dw)


# In[ ]:





# In[ ]:





# In[ ]:




