from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import os

class Grapesaur:
    """_summary_
    """
    def __init__(self, DIR, increaseMemory = False):
        """_summary_

        Args:
            DIR (_type_): _description_
            increaseMemory (bool, optional): _description_. Defaults to False.
        """
        self.DIR = DIR
        self.flatCols = list()
        if not increaseMemory:
            try:
                self.spark = SparkSession.builder.appName("Grapesaur").getOrCreate()
                self.spark.sparkContext.setLogLevel("ERROR")
            except:
                self.__setError(1)
            self.__setError(2)
        else:
            try:
                self.spark = SparkSession.builder \
                    .config("spark.driver.memory", "10g") \
                    .appName("Grapesaur") \
                    .getOrCreate()
                self.spark.sparkContext.setLogLevel("ERROR")
            except:
                self.__setError(1)
            self.__setError(2)
    
    def __del__(self):
        """_summary_
        """
        self.spark.stop()
        self.__setError(3)

    def __setError(self, status):
        """_summary_

        Args:
            status (_type_): _description_
        """
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
        elif status == 6:
            self.error = {
                'status': True,
                'message': "Column name mismatch",
                'resolution': "Please check the column names in thr tree view"
            }
        elif status == 7:
            self.error = {
                'status': True,
                'message': "Parameter field name mismatch",
                'resolution': "Please recheck the entered parameter"
            }

    def __getFiletype(self, fileName):
        """_summary_

        Args:
            fileName (_type_): _description_

        Returns:
            _type_: _description_
        """
        if fileName in os.listdir(self.DIR):
            ext = fileName.split(".")
            ext = ext.pop().lower()
            self.__setError(0)
            return ext
        else:
            self.__setError(5)
            self.showError()
    
    def __getDtype(self, colname, df):
        """_summary_

        Args:
            colname (_type_): _description_
            df (_type_): _description_

        Returns:
            _type_: _description_
        """
        if df == "NA":
            self.__setError(2)
            self.showError()
            return "Not Found"
        self.__setError(0)
        fulldtype = [dtype for name, dtype in df.dtypes if name == colname]
        dtype = fulldtype[0].split('<').pop(0)
        return dtype

    def __searchTrueDF(self, colname, df):
        """_summary_

        Args:
            colname (_type_): _description_
            df (_type_): _description_

        Returns:
            _type_: _description_
        """
        if "." in colname:
            cols = colname.split(".")
            for col in cols:
                if self.__searchTrueDF(col, df) == "NA":
                    self.__setError(6)
                    return "NA"
                df = self.__searchTrueDF(col, df).select(col)
            self.__setError(0)
            return df
        if colname in df.columns:
            self.__setError(0)
            return df
        for col in df.columns:
            dt = self.__getDtype(col, df)
            self.__setError(0)
            if 'struct' in dt:
                tempCol = "{}.*".format(col)
                newDF = df.select(tempCol)
                newcols = newDF.columns
                if colname in newcols:
                    return newDF
                else:
                    return self.__searchTrueDF(colname, newDF)
            elif 'array' in dt:
                temp = self.__searchTrueDF(colname, df.select(explode(df[col])))
                if temp != "NA" or df.columns.index(col) == (len(df.columns)-1):
                    return temp
        self.__setError(6)
        return 'NA'

    def __getFullColumnPath(self, colname):
        """_summary_

        Args:
            colname (_type_): _description_

        Returns:
            _type_: _description_
        """
        if len(self.flatCols) == 0:
            self.flatten()
        actualcolname = None
        if "." not in colname:
            for col in self.flatCols:
                splitnames = col.split(".")
                if colname in splitnames:
                    actualcolname = col
                    break
        else:
            for col in self.flatCols:
                if colname in col:
                    actualcolname = col
                    break
        return actualcolname

    def showError(self):
        """_summary_
        """
        if (self.error['status'] == True):
            print("Error {}, You can resolve the error by: {}".format(self.error['message'], self.error['resolution']))
        else:
            print("No errors reported")

    def readFile(self, fileName,default = False):
        """_summary_

        Args:
            fileName (_type_): _description_
            default (bool, optional): _description_. Defaults to False.

        Returns:
            _type_: _description_
        """
        self.fileName = fileName
        extension = self.__getFiletype(self.fileName)
        if self.error['status'] == False:
            if extension == "json" or extension == "jl":
                df = self.spark.read.json(self.fileName)
            elif extension == "csv":
                df = self.spark.read.csv(self.fileName, header=True)
            else:
                self.__setError(4)
                self.showError()
                return
        else:
            return
        if default == True:
            return df
        else:
            self.showError()
            self.df = df
    
    def getColumnNames(self, colname = None, df = None):
        """_summary_

        Args:
            colname (_type_, optional): _description_. Defaults to None.
            df (_type_, optional): _description_. Defaults to None.

        Returns:
            _type_: _description_
        """
        if df == "NA":
            self.__setError(6)
            self.showError()
            return "No column named {} found".format(colname)
        self.__setError(0)
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
        """_summary_

        Args:
            no (int, optional): _description_. Defaults to 20.
            vertical (bool, optional): _description_. Defaults to False.
            truncate (bool, optional): _description_. Defaults to True.
            colname (_type_, optional): _description_. Defaults to None.
            df (_type_, optional): _description_. Defaults to None.
            all (bool, optional): _description_. Defaults to False.
        """
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
                    self.__setError(6)
                    self.showError()
                else:
                    self.__setError(0)
                    if not all:
                        df.select(colname).show(no, vertical=vertical, truncate=truncate)
                    else:
                        df.select(colname).show(df.select(colname).count(), vertical=vertical, truncate=truncate)
    
    def showUniqueData(self, colname, df = None, desc = True):
        """_summary_

        Args:
            colname (_type_): _description_
            df (_type_, optional): _description_. Defaults to None.
            desc (bool, optional): _description_. Defaults to True.
        """
        if df == None:
            df = self.df
        if "." in colname:
            df = self.__searchTrueDF(colname, df)
            colname = colname.split(".").pop()
        else:
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
        """_summary_
        """
        self.df.printSchema()

    def search(self, searchquery=None, searchfield=None, displayfields = None, show = True, df = None):
        """_summary_

        Args:
            searchquery (_type_, optional): _description_. Defaults to None.
            searchfield (_type_, optional): _description_. Defaults to None.
            displayfields (_type_, optional): _description_. Defaults to None.
            show (bool, optional): _description_. Defaults to True.
            df (_type_, optional): _description_. Defaults to None.
        """
        if df == None:
            df = self.df
        trueDf = self.__searchTrueDF(searchfield, df)
        if trueDf == "NA":
            self.__setError(7)
            self.showError()
            return
        self.__setError(0)
        self.flatten()
        if displayfields == None:
            finalfields = "*"
        else:
            displayfields = [x.strip(" ") for x in displayfields.split(",")]
            fullname = self.__getFullColumnPath(searchfield)
            finalfields = list(["{} as {}".format(fullname, fullname.replace(".", "_"))])
            for mcol in displayfields:
                tempname = self.__getFullColumnPath(mcol)
                finalfields.append("{} as {}".format(tempname, tempname.replace(".", "_")))
            finalfields = ", ".join(finalfields)
        dtype = self.__getDtype(searchfield, trueDf)
        if dtype == 'string':
            query = "select {} from df where {} == '{}'".format(finalfields, searchfield, searchquery)
        else:
            query = "select {} from df where array_contains({}, '{}')".format(finalfields, searchfield, searchquery)
        resultDf = self.sqlQuery(query)
        self.showRows(df = resultDf, all = True, truncate = False)
    
    def flatten(self, df = None, parentColumn = None):
        """_summary_

        Args:
            df (_type_, optional): _description_. Defaults to None.
            parentColumn (_type_, optional): _description_. Defaults to None.
        """
        if df == None:
            df = self.df
        for colname in df.columns:
            subcols = self.getColumnNames(colname, df = df)
            tempMainCol = colname
            tp = str(type(subcols))
            if "list" in tp:
                tempCol = subcols[-1]
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

    def summary(self):
        """_summary_
        """
        print("File selected = {}".format(self.fileName))
        print("="*(20+len(self.fileName)))
        print("Column Names: {}".format(", ".join(self.getColumnNames())))
        print("Column Count: {}".format(len(self.getColumnNames())))
        print("Total Rows: {}".format(self.df.count()))
        # print("Total Duplicates by all Columns: {}".format(self.getDuplicateCount(columns = "addresses, provider, specialties, networks")))
        print('Tree view of the schema:')
        self.tree()

    def sqlQuery(self, query, df = None):
        """_summary_

        Args:
            query (_type_): _description_
            df (_type_, optional): _description_. Defaults to None.

        Returns:
            _type_: _description_
        """
        if df == None:
            df = self.df
        df.createOrReplaceTempView('tempview')
        fullquery = query.replace('df', 'tempview')
        return self.spark.sql(fullquery)

    def getDuplicates(self,count = True , columns = None , df = None):
        """_summary_

        Args:
            count (bool, optional): _description_. Defaults to True.
            columns (_type_, optional): _description_. Defaults to None.
            df (_type_, optional): _description_. Defaults to None.

        Returns:
            _type_: _description_
        """
        if df == None:
            df = self.df
        if columns != None:
            columns = [x.strip() for x in columns.split(',')]
            self.flatten(df)
            toDisplay = list()
            for onefield in columns:
                tempCols = ""
                for col in self.flatCols:
                    col = col.split('.')
                    if onefield in col:
                        tempCols = col
                        break
                if tempCols == "":
                    self.__setError(6)
                    self.showError()
                    return
                tempcolnames = ".".join(tempCols)
                toDisplay.append("{} as {}".format(tempcolnames, onefield))
            toDisplay = ", ".join(toDisplay)
            query = "select distinct {} from df".format(toDisplay)
            df = self.sqlQuery(query)
        else:
            df = df.distinct()
        if count == True:
            noDuplicats = self.df.count() - df.count()
            return noDuplicats
        else:
            # query = " select * from df1 where not exists (select * from df2 where df1 = df2)"
            return self.df.exceptAll(df)

    def removeDuplicates(self,columns = None):
        """_summary_

        Args:
            columns (_type_, optional): _description_. Defaults to None.
        """
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
                    self.__setError(6)
                    self.showError()
                    return
                tempcolnames = ".".join(tempCols)
                toDisplay.append("{} as {}".format(tempcolnames, onefield))
            toDisplay = ", ".join(toDisplay)
            df =  df.dropDuplicates(toDisplay)
        else:
            df = df.dropDuplicates()
        count=self.getDuplicates(count=False)
        print("Distinct count: "+str(df.count()))
        self.showRows(all = True, truncate=False, df=count)   

    def convertFile(self, type, separator):
        """_summary_

        Args:
            type (_type_): _description_
            separator (_type_): _description_
        """
        convert = self.df
        # print("New file name = {}".format(self.fileName.split('.')[0] + '.' + type))
        # convert.repartition(1).write.csv(self.fileName.split('.')[0] + '.' + type,sep = seperator)
        convert.coalesce(1).write.csv(self.fileName.split('.')[0] + '.' + type, sep = separator)

    def compareTwoDatasets(self,old_df):
        """_summary_

        Args:
            old_df (_type_): _description_
        """
        old = self.readFile(old_df, True)
        new = self.df
        difference = old.exceptAll(new)
        self.showRows(all=True, df = difference, truncate= False )
        print("Total Missing = {}".format(difference.count()))