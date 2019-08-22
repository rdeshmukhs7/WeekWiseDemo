
# coding: utf-8

# In[25]:

import adal
import requests
import os
import json
import base64


# In[26]:

tenant = '524b0e96-35a3-46ef-ade3-a76c1936a890'
authority_url = 'https://login.microsoftonline.com/' + tenant
client_id = 'e0ab26b2-77cb-4ff5-b811-4b91656b735a'
client_secret = 'JkZQkfeJBY2f5-8w4YjcVuwpFEXyT@*.'
resource = 'https://management.azure.com/'
subscription_id = '5322a3d3-c098-4bb6-bbae-652fc50f0873'


# In[27]:

context = adal.AuthenticationContext(authority_url)
token = context.acquire_token_with_client_credentials(resource, client_id, client_secret)


# In[28]:

headers = {'Authorization': 'Bearer ' + token['accessToken'], 'Content-Type': 'application/json'}
params = {'api-version': '2019-05-10'}

resourceData = {'location':'eastus'}


# In[7]:

resourceurl = 'https://management.azure.com/subscriptions/'+subscription_id+'/resourcegroups/restwitterrd7'
resourceResponse = requests.put(resourceurl,data = json.dumps(resourceData) ,headers=headers, params=params)


print(resourceResponse.status_code)
print(json.dumps(resourceResponse.json(), indent=4, separators=(',', ': ')))


# In[10]:

# to create databricks
resourceGroupName='restwitterrd7'
workspaceName = 'databricksTwitter'
location = 'centralus'
url = 'https://management.azure.com/subscriptions/'+subscription_id+'/resourcegroups/'+resourceGroupName+'/providers/Microsoft.Databricks/workspaces/'+workspaceName
data = {
           'properties':{
               'managedResourceGroupId':'/subscriptions/'+subscription_id+'/resourcegroups/myManagedRGTwitterDatafactory'
           },
           'name':workspaceName,
           'location': location
       }
r = requests.put(url, data=json.dumps(data),headers=headers, params = {'api-version':'2018-04-01'})
print(json.dumps(r.json(), indent=4, separators=(',', ': ')))


# In[14]:

# to generate databricks token
domain = location + '.azuredatabricks.net'
# main token expiry 90 days. set by UI. Every time create the databricks add new value of token
tokenDB = b'dapieb2130b9f5ba117ab526f3deb9972977' 

response=requests.post(
 'https://'+domain+'/api/2.0/token/create',
  headers={'Authorization': b"Basic " + base64.standard_b64encode(b"token:" + tokenDB)},
 json={
 "lifetime_seconds": 86400, # databricks token expiry of 1 day
 "comment": "token generated for Twitter Data factory Python Application"}
)
tokenDBregenerate =(response.json()['token_value'])
print(response.json())


# In[15]:

headersDB = {'Authorization': b"Basic " + base64.standard_b64encode(b"token:" + tokenDBregenerate)}

# to create cluster inside databricks
response = requests.post(
 'https://'+domain+'/api/2.0/clusters/create',
 headers={'Authorization': b"Basic " + base64.standard_b64encode(b"token:" + tokenDBregenerate)},
 json={
       "num_workers":4,
       "cluster_name": "clusterTwitterPython",
       "spark_version": "5.3.x-scala2.11",
       "node_type_id": "Standard_D3_v2",
       "spark_env_vars": {
       "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
     }
 }
)
if response.status_code == 200:
 print(response.json()['cluster_id'])
else:
 print("Error launching cluster: %s: %s" % (response.json()["error_code"], response.json()["message"]))


# In[16]:

#store cluster id in variable for feature reference
cluster_id=response.json()['cluster_id']
#cluster_id="0820-061457-ohms330"
print(cluster_id)


# In[18]:

# to import jupyter notebook inside cluster from local environment
# to create folder inside databrick workspace

foldername='TwitterProjectRd7'
response = requests.post('https://'+domain+'/api/2.0/workspace/mkdirs',
   headers=headersDB,    
   json ={'path':'/Users/shalu.rajpoot1@cgm4l.onmicrosoft.com/'+foldername}
)
print(response.status_code)


# In[19]:

# In[12]:

# local python file path 
response = requests.post(
   'https://'+domain+'/api/2.0/dbfs/create',
   headers=headersDB,
   json={"path": "/temp/upload_large_file", "overwrite": "true"}
)

print(response.json()['handle'])
handle = response.json()['handle']

# local machine file path
file = open('/home/admin1/Downloads/twitter-sentiment-analysis2/TwitterPredictionTest.py')
block = file.read(1<<20)
block = str.encode(block)
data = base64.standard_b64encode(block)
# print(data)
# print(data.decode('utf-8'))

response = requests.post(
   'https://'+domain+'/api/2.0/dbfs/add-block',
   headers=headersDB,
   json={"handle": handle, "data": data.decode('utf-8')}
)



# In[20]:

# import local code notebook inside databricks

json = {
 "content": data.decode('utf-8'),
 "path": "/Users/shalu.rajpoot1@cgm4l.onmicrosoft.com/"+foldername+"/TwitterFile",
 "language": "PYTHON",
 "overwrite": True,
 "format": "SOURCE"
}
response = requests.post(
   'https://'+domain+'/api/2.0/workspace/import',
   headers=headersDB,
   json=json
)
print(response.status_code)


# In[21]:

# install libraries inside cluster

json = {
   "cluster_id":cluster_id,
   "libraries":[        
       {
           "pypi": {
           "package": "azure"            
         }
       },
       {
           "pypi": {
           "package": "nltk"            
         }
       }
   ]
}
response = requests.post(
   'https://'+domain+'/api/2.0/libraries/install',
   headers = headersDB,
   json = json
)

print(response.status_code)



# In[29]:

# to create data factory


datafactoryname = "datafactorytwitterapp"
url = "https://management.azure.com/subscriptions/"+subscription_id+"/resourceGroups/"+resourceGroupName+"/providers/Microsoft.DataFactory/factories/"+datafactoryname
data ={
   "name":datafactoryname,
   "location": "central us"
}
response = requests.put(url, data=json.dumps(data), headers= headers, params={'api-version':'2018-06-01'})
print(response.status_code)
print(json.dumps(response.json(), indent=4, separators=(',', ': ')))



# In[30]:

# to create databricks notebook linked services


linkedServiceName = "AzureDatabricksLinkedService"
url = "https://management.azure.com/subscriptions/"+subscription_id+"/resourceGroups/"+resourceGroupName+"/providers/Microsoft.DataFactory/factories/"+datafactoryname+"/linkedservices/"+linkedServiceName
data={
   "name":linkedServiceName, # name of the databricks
   "properties": {
   "type": "AzureDatabricks",
   "typeProperties":{
       "domain": "https://"+location+".azuredatabricks.net",
       "existingClusterId":cluster_id,    
       "accessToken":{
           "type":"SecureString",
           "value":tokenDBregenerate.decode('utf-8')
       }
   }    
 }
}
response = requests.put(url,data=json.dumps(data),headers=headers, params={'api-version':'2018-06-01'})
print(response.status_code)
print(json.dumps(response.json(), indent=4, separators=(',', ': ')))



# In[31]:

# to create pipeline


pipelinename="pipelinetwitterapplication"
url = "https://management.azure.com/subscriptions/"+subscription_id+"/resourceGroups/"+resourceGroupName+"/providers/Microsoft.DataFactory/factories/"+datafactoryname+"/pipelines/"+pipelinename
data = {
 "name":pipelinename,
 "properties":{
       "activities":[
           {
               "name":"DatabricksActivity",
               "type":"DatabricksNotebook",
               "linkedServiceName":{
                   "referenceName":linkedServiceName,
                   "type":"LinkedServiceReference"
               },
               "typeProperties":{
                   "notebookPath":"/Users/shalu.rajpoot1@cgm4l.onmicrosoft.com/TwitterProjectRd7/TwitterFile"
               }
           }
       ]
   }
}
response=requests.put(url,data=json.dumps(data), headers=headers,params={'api-version':'2018-06-01'})
print(response.status_code)
print(json.dumps(response.json(), indent=4, separators=(',', ': ')))


# In[32]:

# run pipeline


url = "https://management.azure.com/subscriptions/"+subscription_id+"/resourceGroups/"+resourceGroupName+"/providers/Microsoft.DataFactory/factories/"+datafactoryname+"/pipelines/"+pipelinename+"/createRun"
response = requests.post(url, headers=headers, params={'api-version':'2018-06-01'})
print(response.status_code)
print(json.dumps(response.json(), indent=4, separators=(',', ': ')))


# In[ ]:



