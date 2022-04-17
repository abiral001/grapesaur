from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import os

class Grapesaur:
    def __init__(self, DIR):
        self.DIR = DIR
        self.flatCols = list()
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
        if df == "NA":
            return "Not Found"
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
                    elements = df.select(explode(df[colname]).alias(colname))
                    columnNames = self.getColumnNames(colname, elements)
                    return columnNames
                else:
                    return colname
            else:
                return self.getColumnNames(colname, self.__searchTrueDF(colname, df))

    def showRows(self, no = 20, vertical = False, truncate = True, colname = None, df = None, all = False):
        if df == None:
            df = self.df
        if colname == None:
            if not all:
                df.show(no, vertical=vertical, truncate=truncate)
            else:
                df.show(df.count(), vertical=vertical, truncate=truncate)
        else:
            if colname in df.columns:
                if not all:
                    df.select(colname).show(no, vertical=vertical, truncate=truncate)
                else:
                    df.select(colname).show(df.select(colname).count(), vertical=vertical, truncate=truncate)
            else:
                df = self.__searchTrueDF(colname, df)
                if df == "NA":
                    print("No column named {} found".format(colname))
                else:
                    if not all:
                        df.select(colname).show(no, vertical=vertical, truncate=truncate)
                    else:
                        df.select(colname).show(df.select(colname).count(), vertical=vertical, truncate=truncate)
    
    def showUniqueData(self, colname, df = None, desc = True):
        if df == None:
            df = self.df
        if colname not in df.columns:
            df = self.__searchTrueDF(colname, df)
        dt = self.__getDtype(colname, df)
        if dt == "struct":
            colname = "{}.*".format(colname)
            df = df.select(colname)
            self.showRows(all=True, truncate = True, df=df)
        else:
            df = df.select(colname)
            if desc:
                df = df.groupby(colname).count().orderBy(col('count').desc())
            else:
                df = df.groupby(colname).count().orderBy(col('count'))
            self.showRows(all=True, truncate = False, df=df)

    def tree(self):
        self.df.printSchema()

    def search(self, searchquery=None, searchfield=None, displayfields = None, show = True):
        df = self.df
        tempdf = self.__searchTrueDF(searchfield, df)
        if tempdf == 'NA':
            return "The searchfield is not found"
        self.flatten()
        allCols = ""
        for col in self.flatCols:
            col = col.split('.')
            if searchfield in col:
                allCols = col
                break
        if allCols == "":
            return "No search field found"
        resultDf = df
        for col in allCols:
            resultDf = resultDf[col]
        if searchquery != None:
            finaldf = df.filter(resultDf.rlike(searchquery))
        else:
            finaldf = df
        if displayfields != None:
            displayfields = [x.strip() for x in displayfields.split(',')]
            toDisplay = list()
            for onefield in displayfields:
                tempCols = ""
                for col in self.flatCols:
                    col = col.split('.')
                    if onefield in col:
                        tempCols = col
                        break
                if tempCols == "":
                    return "{} field not found".format(onefield)
                tempcolnames = ".".join(tempCols)
                toDisplay.append("{} as {}".format(tempcolnames, onefield))
            tempcolnames = ".".join(allCols)
            toDisplay.append("{} as {}".format(tempcolnames, searchfield))
            query = "select {} from df".format(", ".join(toDisplay))
            finaldf = self.sqlQuery(query, finaldf)
        if show:
            self.showRows(all=True, df = finaldf, truncate = False)
        else:
            return finaldf
    
    def flatten(self, df = None, parentColumn = None):
        if df == None:
            df = self.df
        for colname in df.columns:
            count = 0
            subcols = self.getColumnNames(colname, df = df)
            tempMainCol = colname
            tp = str(type(subcols))
            if "list" in tp:
                tempCol = subcols[0]
                newDf = self.__searchTrueDF(tempCol, df)
                if parentColumn == None:
                    self.flatten(df = newDf, parentColumn = tempMainCol)
                else:
                    self.flatten(df = newDf, parentColumn = parentColumn+"."+tempMainCol)
            else:
                if parentColumn == None:
                    self.flatCols.append(subcols)
                else:
                    self.flatCols.append(parentColumn+"."+subcols)
            count += 1

    def summary(self):
        print("File selected = {}".format(self.fileName))
        print("="*(20+len(self.fileName)))
        print("Column Names: {}".format(", ".join(self.getColumnNames())))
        print("Column Count: {}".format(len(self.getColumnNames())))
        print("Total Rows: {}".format(self.df.count()))
        print("Total Duplicates by all Columns: {}".format(self.getDuplicateCount()))
        print('Tree view of the schema:')
        self.tree()

    def sqlQuery(self, query, df = None):
        if df == None:
            df = self.df
        df.createOrReplaceTempView('tempview')
        fullquery = query.replace('df', 'tempview')
        return self.spark.sql(fullquery)

    def getDuplicateCount(self, columns = None):
        df = self.df
        if columns != None:
            columns = [x.strip() for x in columns.split(',')]
            self.flatten()
            toDisplay = list()
            for onefield in columns:
                tempCols = ""
                for col in self.flatCols:
                    col = col.split('.')
                    if onefield in col:
                        tempCols = col
                        break
                if tempCols == "":
                    return "{} field not found".format(onefield)
                tempcolnames = ".".join(tempCols)
                toDisplay.append("{} as {}".format(tempcolnames, onefield))
            toDisplay = ", ".join(toDisplay)
            query = "select distinct {} from df".format(toDisplay)
            df = self.sqlQuery(query)
        else:
            df = df.distinct()
        noDuplicats = self.df.count() - df.count()
        return noDuplicats

    def removeDuplicates(self):
        #send to another just name this one readOnlyOperations and the other writeOperations
        pass

    def convertFile(self):
        #send to process
        pass

    def compareTwoDatasets(self):
        # send this to another module this module is already cluttered as is
        pass