# Databricks notebook source
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

# Importing the training set
dataset_train = pd.read_csv('https://sapramod.blob.core.windows.net/cpramod/Google_Stock_Price_Train.csv')
training_set = dataset_train.iloc[:, 1:2].values

print(dataset_train)

# COMMAND ----------

from sklearn.preprocessing import MinMaxScaler
sc = MinMaxScaler(feature_range = (0, 1))
training_set_scaled = sc.fit_transform(training_set)

# Creating a data structure with 60 timesteps and 1 output
X_train = []
y_train = []
for i in range(60, 1258):
    X_train.append(training_set_scaled[i-60:i, 0])
    y_train.append(training_set_scaled[i, 0])
X_train, y_train = np.array(X_train), np.array(y_train)

# COMMAND ----------

from sklearn.linear_model import LinearRegression

regressor = LinearRegression()
regressor.fit(X_train,y_train)

# COMMAND ----------

import pickle
filename = 'finalized_model.sav'
pickle.dump(regressor, open(filename, 'wb'))

# COMMAND ----------

loaded_model = pickle.load(open('finalized_model.sav', 'rb'))

# COMMAND ----------

result = loaded_model.predict(X_train)
print(result)

# COMMAND ----------

dataset_test = pd.read_csv('https://sapramod.blob.core.windows.net/cpramod/Google_Stock_Price_Test.csv')
training_set = dataset_test.iloc[:, 1:2].values
X_test = []
for i in range(5, 20):
    X_test.append(training_set_scaled[i-5:i, 0])
    

# COMMAND ----------

# MAGIC %sh
# MAGIC /databricks/python/bin/pip install --upgrade pip

# COMMAND ----------

# MAGIC %sh
# MAGIC /databricks/python/bin/pip install azure

# COMMAND ----------


from azure.storage.blob import BlockBlobService
from azure.storage.blob import ContentSettings

block_blob_service = BlockBlobService(account_name='sapramod', account_key='2l9jLxz/FSbJILVy4cLjoNnmcH4D8XbDoK/GTCZ9QO1qp5bmaaks8oizGWNW+unmgRno6d8n0U79rXVNixHgUA==')
block_blob_service.create_container('cpramod')

#Upload the CSV file to Azure cloud
block_blob_service.create_blob_from_path(
    'cpramod',
    'finalized_model.sav',
    'finalized_model.sav',
    content_settings=ContentSettings(content_type='application/SAV')
            )

# COMMAND ----------

loaded_model = open('finalized_model.sav', 'rb')



# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://<cpramod>@<adbpramod>.blob.core.windows.net",
  mount_point = "/mnt/<mount-name>",
  extra_configs = {"<conf-key>":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})

# COMMAND ----------


