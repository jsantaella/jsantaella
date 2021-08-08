from pyspark.sql import SparkSession
import json 
from pyspark.sql.functions import udf
from pyspark.sql.functions import monotonically_increasing_id 
from collections import OrderedDict
import datetime
from pyspark.sql.functions import *

spark = SparkSession.builder\
        .master("local")\
        .appName("Colab")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()

#Function that reads the json file it contains the guidance of columns
def obtain_data_config(path):
	with open(path) as f:
		data=json.load(f)
	return data


def getColumnsList(inputDict):
  '''
    Collects the value attribute of dict layouts and create a tuple for every column stores it in lists.
            Parameters:
                    inputDict (dict): dictionary that contains "columnName":"length" key value pair for every layout.
            Returns:
                    outputColumns (list): list that contains a tuple with the following pattern: (start, length, nameOfColumn )
    '''
    initLen=1
    outputColumns=[]
    for k,v in inputDict.items():
        outputColumns.append((initLen,v,k))
        initLen+=v
    return outputColumns

  
  
#Capturing the metadata of columns  
file=obtain_data_config("metadata_config.json")
layoutDict=file.get('Detail1').get('Layout')
guidedColumns=getColumnsList(layoutDict)

#First option using withColumn()
df=spark.read.format('csv').load('FIXED_WIDTH_EXAMPLE')
df=df.filter(((df._c0.substr(1,1)!='H') & (df._c0.substr(1,1)!='T') ))
df.show(20, truncate=False)
for item in guidedColumns: 
    df=df.withColumn(item[2], df._c0.substr(item[0],item[1]))
    df.show()
    
df.drop("_c0").show()


def getColumnsSelectFunc(inputDict):
  '''
    Collects the value attribute of dict layouts and create a tuple for every column stores it in lists.
            Parameters:
                    inputDict (dict): dictionary that contains "columnName":"length" key value pair for every layout.
            Returns:
                    outputColumns (list): list that contains a tuple with the following pattern: (start, length, nameOfColumn )
    '''
    initLen=1
    outputColumns=[]
    for k,v in inputDict.items():
        outputColumns.append(substring("_c0",initLen,v).alias(k))
        initLen+=v
    return outputColumns
  

#Second process using select() function
df2=spark.read.format('csv').load('FIXED_WIDTH_EXAMPLE')
cols1=[substring("_c0",x[0],x[1]).alias(x[2]) for x in guidedColumns]
cols2=getColumnsSelectFunc(layoutDict)
df2=df2.select(cols2)
df2.show()
