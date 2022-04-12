from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import os

class Grapesaur:
    def __init__(self, DIR):
        self.DIR = DIR
        try:
            self.spark = SparkSession.builder.appName("Grapesaur").getOrCreate()
            self.spark.sparkContext.setLogLevel("ERROR")
        except:
            self.__setError(1)
        self.__setError(2)
    
    def __del__(self):
        self.spark.stop()
        self.__setError(3)

    def __setError(self, status):
        if status == 0:
            self.error = {
                'status': False,
                'message': "No errors reported",
                'resolution': "Nothing to do"
            }
        elif status == 1:
            self.error = {
                'status': True,
                'message': "PySpark failed to initialize",
                'resolution': "Please check the install path for PySpark"
            }
        elif status == 2:
            self.error = {
                'status': True,
                'message': "No dataset provided",
                'resolution': "Please provide data using readFile(...)"
            }
        elif status == 3:
            self.error = {
                'status': True,
                'message': "Pyspark is not initialized",
                'resolution': "Please restart the application"
            }
        elif status == 4:
            self.error = {
                'status': True,
                'message': "File type not supported",
                'resolution': "Please contact developer for the required dataset type"
            }
        elif status == 5:
            self.error = {
                'status': True,
                'message': "File not found",
                'resolution': "Please place the file in the same directory as the main caller"
            }

    def __getFiletype(self, fileName):
        if fileName in os.listdir(self.DIR):
            ext = fileName.split(".")
            ext = ext.pop().lower()
            self.__setError(0)
            return ext
        else:
            self.__setError(5)
            self.showError()
    
    def __getDtype(self, colname, df):
        fulldtype = [dtype for name, dtype in df.dtypes if name == colname]
        dtype = fulldtype[0].split('<').pop(0)
        return dtype

    def __searchTrueDF(self, colname, df):
        if colname in df.columns:
            return df
        for col in df.columns:
            dt = self.__getDtype(col, df)
            if dt == 'struct':
                tempCol = "{}.*".format(col)
                newDF = df.select(tempCol)
                newcols = newDF.columns
                if colname in newcols:
                    return newDF
                else:
                    return self.__searchTrueDF(colname, newDF)
            elif dt == 'array':
                temp = self.__searchTrueDF(colname, df.select(explode(df[col])))
                if temp != "NA" or df.columns.index(col) == (len(df.columns)-1):
                    return temp
        return 'NA'

    def showError(self):
        if (self.error['status'] == True):
            print("Error {}, You can resolve the error by: {}".format(self.error['message'], self.error['resolution']))
        else:
            print("No errors reported")

    def readFile(self, fileName):
        self.fileName = fileName
        extension = self.__getFiletype(self.fileName)
        if self.error['status'] == False:
            if extension == "json":
                df = self.spark.read.json(self.fileName)
            elif extension == "csv":
                df = self.spark.read.csv(self.fileName)
            else:
                self.__setError(4)
                self.showError()
                return
        else:
            return
        self.showError()
        self.df = df
    
    def getColumnNames(self, colname = None, df = None):
        if df == "NA":
            return "No column named {} found".format(colname)
        if df == None:
            df = self.df
        if colname == None:
            columnNames = df.columns
            return columnNames
        else:
            if colname in df.columns:
                dt = self.__getDtype(colname, df)
                if dt == 'struct':
                    colname = "{}.*".format(colname)
                    columnNames = df.select(colname).columns
                    return columnNames
                elif dt == 'array':
                    elements = df.select(explode(df[colname]))
                    columnNames = self.getColumnNames('col', elements)
                    return columnNames
                else:
                    return colname
            else:
                return self.getColumnNames(colname, self.__searchTrueDF(colname, df))
    
    def showColumnData(self, colname):
        self.df.networks.show()

    def showData(self, no = 20, vertical = False, truncate = True, colname = None):
        if colname == None:
            self.df.show(no, vertical=vertical, truncate=truncate)
        else:
            self.df.select(colname).show(no, vertical=vertical, truncate=truncate)

    def showFullSchema(self):
        self.df.printSchema()