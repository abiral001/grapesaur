from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
import sys
import subprocess

from setuptools import Command

sys.path.append('.')

from modules.error import Error

class GDataAnalysis:
    """Used for processing datasets using pyspark
    """
    def __init__(self, DIR, increaseMemory = False):
        """constructor for GDA

        Args:
            DIR (_type_): Default directory from which datasets can be pulled
            increaseMemory (bool, optional): Flag to either use default 2G of RAM(False) or higher 10G(True). Defaults to False.
        """
        self.DIR = DIR
        self.newfileName = None
        self.flatCols = list()
        self.err = Error()
        if not increaseMemory:
            try:
                self.spark = SparkSession.builder.appName("GDA").getOrCreate()
                self.spark.sparkContext.setLogLevel("ERROR")
            except:
                self.__setError(1)
            self.__setError(2)
        else:
            try:
                self.spark = SparkSession.builder \
                    .config("spark.driver.memory", "10g") \
                    .appName("GDA") \
                    .getOrCreate()
                self.spark.sparkContext.setLogLevel("ERROR")
            except:
                self.__setError(1)
            self.__setError(2)
    
    def __del__(self):
        """Destructor for GDA
        """
        self.spark.stop()
        self.__setError(3)
        if self.newfileName != None:
            os.remove(self.newfileName)


    def __setError(self, status):
        """Internal function that sets different error at different program execution step

        Args:
            status (int): Denotes the error code
        """
        self.err.setError(status)

    def __getFiletype(self, fileName):
        """Internal Function that is used to get the file extension of the supplied file

        Args:
            fileName (string): Filename in the same directory as example.py

        Returns:
            string: Extension of the file
        """
        if fileName in os.listdir(self.DIR):
            ext = fileName.split(".")
            ext = ext.pop().lower()
            self.ext = ext
            self.__setError(0)
            return ext
        else:
            self.__setError(5)
            self.showError()
    
    def __getDtype(self, colname, df):
        """Internal Function to obtain the type of data stored in a particular column of the dataset

        Args:
            colname (string): The name of column which exists in the dataset
            df (spark.df): The dataframe under observation

        Returns:
            string: The type of data stored in the colname column of the dataset
        """
        if df == None:
            self.__setError(2)
            self.showError()
            return "Not Found"
        self.__setError(0)
        if "." in colname:
            colname = colname.split(".").pop()
        fulldtype = [dtype for name, dtype in df.dtypes if name == colname]
        dtype = fulldtype[0].split('<').pop(0)
        return dtype

    def __searchTrueDF(self, colname, df):
        """Internal recursion function which is used to return the dataframe which contains the colname as column. The column can be in any level of the dataframe
        Args:
            colname (string): The column to search in the dataframe
            df (spark.df): The dataframe to search the column in

        Returns:
            spark.df: The immediate dataframe which contains colname as one of its column
        """
        if "." in colname:
            allCols = colname.split('.')
            for idx, col in enumerate(allCols):
                if idx == len(allCols)-1:
                    break
                df = self.__searchTrueDF(colname = col, df = df)
                if df == "NA":
                    self.__setError(6)
                    return "NA"
            self.__setError(0)
            return df
        if colname in df.columns:
            dt = self.__getDtype(colname = colname, df = df)
            if 'struct' in dt:
                colname = "{}.*".format(colname)
                df = df.select(colname)
            elif 'string' in dt:
                df = df.select(colname)
            elif 'array' in dt:
                df = self.__searchTrueDF(colname, df.select(explode(df[colname]).alias(colname)))
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
                    self.__setError(0)
                    return newDF
                else:
                    return self.__searchTrueDF(colname, newDF)
            elif 'array' in dt:
                temp = self.__searchTrueDF(colname, df.select(explode(df[col])))
                if temp != "NA" or df.columns.index(col) == (len(df.columns)-1):
                    self.__setError(0)
                    return temp
        self.__setError(6)
        return "NA"
        

    def __getFullColumnPath(self, colname):
        """Intenal function which return the full path to get the required column name in the default dataframe

        Args:
            colname (string): The column name to return the full path of

        Returns:
            string: The full path to the colname column
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
        """Function to return the set error
        """
        self.err.showError()

    def readFile(self, fileName, default = False):
        """Function that reads the dataset into spark dataframe

        Args:
            fileName (string): The name of the dataset
            default (bool, optional): Flag which denotes whether to return the dataframe(True) or not(False). Defaults to False.

        Returns:
            spark.df or None: Returns the read dataframe if default flag is set
        """
        self.fileName = fileName
        extension = self.__getFiletype(self.fileName)
        if self.err.error['status'] == False:
            if extension == "json" or extension == "jl":
                df = self.spark.read.json(self.fileName)
            elif extension == "csv":
                df = self.spark.read.csv(self.fileName, header=True, multiLine=True, escape="\"")
            elif extension == 'xlsx':
                self.newfileName = 'temp.csv'
                file=open(self.newfileName,'w+')
                file.close()
                command = "xlsx2csv '" + self.fileName + "' " + self.newfileName + ' 2>&1'
                os.system(command)
                df = self.spark.read.csv(self.newfileName, header=True)
            else:
                self.__setError(4)
                self.showError()
                return
        else:
            return
        self.__setError(0)
        self.showError()
        if default == True:
            return df
        else:
            self.df = df
    
    def getColumnNames(self, colname = None, df = None):
        """Function to get all the column names of the provided df

        Args:
            colname (string, optional): If supplied searches the dataframe for this column and returns all the column names inside this column. Defaults to None.
            df (_type_, optional): Optionally can denote which dataframe to start searching on. Defaults to None.

        Returns:
            list: Column names
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
        """Function to display the rows

        Args:
            no (int, optional): How many rows to display?. Defaults to 20.
            vertical (bool, optional): Should the rows be displayed vertically, one at a time?. Defaults to False.
            truncate (bool, optional): Should the table contain only the first few characters of the data in the field?. Defaults to True.
            colname (_type_, optional): Should only particular column be displayed?. Defaults to None.
            df (_type_, optional): Where to display data from?. Defaults to None.
            all (bool, optional): Should all data be displayed?. Defaults to False.
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
    
    def showUniqueData(self, colname, df = None, desc = True, returnDf = False):
        """Function to display only unique data from a particular column with the number of times this data is recurrent in the dataset

        Args:
            colname (_type_): The column to collect data on
            df (_type_, optional): Which dataset to check the column on?. Defaults to None.
            desc (bool, optional): Should the data returned be arranged in descending order?. Defaults to True.
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
            if returnDf == True:
                return df
            else:
                self.showRows(all=True, truncate = True, df=df)
        else:
            df = df.select(colname)
            if desc:
                df = df.groupby(colname).count().orderBy(col('count').desc())
            else:
                df = df.groupby(colname).count().orderBy(col('count'))
            if returnDf == True:
                return df
            else:
                self.showRows(all=True, truncate = False, df=df)

    def tree(self, df = None):
        """Simple function to show the nested columns in the dataset
        """
        if df == None:
            df = self.df
        df.printSchema()

    def search(self, searchquery=None, searchfield=None, displayfields = None, show = True, df = None):
        """A powerful function which can search the dataset for any data in any field supplied

        Args:
            searchquery (string, optional): What to search?. Defaults to None.
            searchfield (string, optional): Which field to search? Please supply the columns with full column path separating each column name with "." PS: Use flatten to get self.flatCols. Defaults to None.
            displayfields (string, optional): Which fields to display from the search results?. Defaults to None.
            show (bool, optional): Should the data be displayed or just the searched data be returned in a dataframe. Defaults to True.
            df (spark.df, optional): Where to search data in?. Defaults to None.
        
        Returns:
            None or spark.df: Returns nothing or the searched dataframe
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
            query = "select {} from df where {} rlike '{}'".format(finalfields, searchfield, searchquery)
        else:
            query = "select {} from df where array_contains({}, '{}')".format(finalfields, searchfield, searchquery)
        print(query)
        resultDf = self.sqlQuery(query)
        if show:
            self.showRows(df = resultDf, all = True, truncate = False)
        else:
            return resultDf
    
    def flatten(self, df = None, parentColumn = None):
        """Recursive function to populate the internal flatfields with the column name required for search function

        Args:
            df (spark.df, optional): The dataframe to use the flatten on. Defaults to None.
            parentColumn (string, optional): Just a variable to be used during recursion to store the proper column name. Defaults to None.
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

    def summary(self, stat = False):
        """As the name specifies summary
        """
        print("File selected = {}".format(self.fileName))
        print("="*(20+len(self.fileName)))
        print("Column Names: {}".format(", ".join(self.getColumnNames())))
        print("Column Count: {}".format(len(self.getColumnNames())))
        totalCount = self.df.count()
        print("Total Rows: {}".format(totalCount))
        if stat == True:
            print("Stats: Null Count in each column")
            self.flatten()
            columns = self.flatCols
            fullStats = list()
            for col in columns:
                try:
                    full = self.showUniqueData(col, returnDf = True)
                    cols = self.getColumnNames(df = full)
                    query = "select * from df where {} is null".format(cols[0])
                    temp = self.sqlQuery(query, df = full)
                    query = "select sum(count) from df"
                    total = self.sqlQuery(query, df = full)
                    total = total.collect()[0]['sum(count)']
                    if temp.count() == 0:
                        data = {
                            "Column Name": "{}".format(col),
                            "Null Count": 0,
                            "Non-null Count": total,
                            "Status": "No null found here"
                        }
                        fullStats.append(data)
                    else:
                        row = temp.collect()[0]
                        data = {
                            "Column Name": "{}".format(col),
                            "Null Count": row['count'],
                            "Non-null Count": total - row['count'],
                            "Status": "Field could be partially null"
                        }
                        fullStats.append(data)
                except:
                    data = {
                        "Column Name": "{}".format(col),
                        "Null Count": totalCount,
                        "Non-null Count": 0,
                        "Status": "Field is fully empty"
                    }
                    fullStats.append(data)
            rdd = self.spark.sparkContext.parallelize(fullStats)
            newcols = [
                StructField('Column Name', StringType(), True), 
                StructField('Null Count', IntegerType(), True), 
                StructField('Non-null Count', IntegerType(), True),
                StructField('Status', StringType(), True),
            ]
            dfcolumns = StructType(newcols)
            new_df = self.spark.createDataFrame(data = rdd, schema=dfcolumns)
            self.showRows(all = True, df = new_df, truncate=False)
        print('Tree view of the schema:')
        self.tree()

    def sqlQuery(self, query, df = None):
        """Can process sql query directly but only for one dataframe for now

        Args:
            query (string): SQL Query. Use df for name for the table
            df (spark.df, optional): The dataframe to process the query on. Defaults to None.

        Returns:
            spark.df: Dataframe obtained after running the query
        """
        if df == None:
            df = self.df
        df.createOrReplaceTempView('tempview')
        fullquery = query.replace('df', 'tempview')
        return self.spark.sql(fullquery)

    def getDuplicates(self,count = True , columns = None , df = None):
        """Function to get the duplicates, either count or the actual duplicates encountered in the dataset

        Args:
            count (bool, optional): Should only the number of duplicates be returned?. Defaults to True.
            columns (string, optional): Which columns to use to check for duplicates?. Defaults to None.
            df (spark.df, optional): Where to check duplicates on?. Defaults to None.

        Returns:
            int or spark.df: Duplicate count or full dataframe of duplicates
        """
        if df == None:
            df = self.df
        toDisplay = "*"
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
            query = "select {} from df".format(toDisplay)
            fulldf = self.sqlQuery(query)
            # query = " select * from df1 where not exists (select * from df2 where df1 = df2)"
            return fulldf.exceptAll(df)

    def removeDuplicates(self, columns = None):
        """Function to remove the duplicates from the dataset and display the duplicates removed

        Args:
            columns (string, optional): Columns to check for duplicates. Defaults to None.
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
                toDisplay.append("{}".format(tempcolnames))
            toDisplay = ", ".join(toDisplay)
            df =  df.dropDuplicates(toDisplay)
        else:
            df = df.dropDuplicates()
        count = self.getDuplicates(count=False, columns = columns)
        self.df = df
        print("Distinct count: "+str(df.count()))
        self.showRows(all = True, truncate=False, df=count)   

    def convertFile(self, type, separator):
        """Function to convert the file type of supplied dataframe (ONly for csv)

        Args:
            type (string): Extension of the file to be the output
            separator (char): What should be the separater for the file?
        """
        if self.ext != 'csv':
            self.__setError(8)
            self.showError()
            return
        convert = self.df
        # print("New file name = {}".format(self.fileName.split('.')[0] + '.' + type))
        # convert.repartition(1).write.csv(self.fileName.split('.')[0] + '.' + type,sep = seperator)
        convert.coalesce(1).write.csv(self.fileName.split('.')[0] + '.' + type, sep = separator)

    def compareTwoDatasets(self, old_df, missing = True):
        """Function to display missing data from new dataset which exists in old dataset

        Args:
            old_df (spark.df): The old dataset to compare
            missing (bool): True => Missing data from new dataset are displayed; False => Common data between two datasets are displayed. DEFAULT: True
        """
        old = self.readFile(old_df, True)
        new = self.df
        if missing == True:
            difference = old.exceptAll(new)
            term = "Missing"
        else:
            difference = old.intersectAll(new)
            term = "Common"
        self.showRows(all=True, df = difference, truncate= False )
        print("Total {} = {}".format(term, difference.count()))