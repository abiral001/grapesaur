import os
from modules.gs import GDataAnalysis
from modules.__version__ import __version__

if __name__ == "__main__":
    # Creating the object
    gs = GDataAnalysis(os.getcwd(), increaseMemory=True)

    # Read File : this is a dummy file
    gs.readFile("202207180944-gersoncompany_com_products_20220718.csv")

    # Get Column Names
    # print(gs.getColumnNames())

    # Show Rows
    # gs.showRows(truncate=False, no = 4)

    # Show data count of unique data
    gs.showUniqueData('Category')
    # gs.showUniqueData('Commenter_Name')

    # Tree view of the dataset
    # gs.tree()

    # Search for a particular data inside the dataset
    # gs.search('null', searchfield='Unit')
    # gs.search('100600', searchfield='sku')

    # Get Column names to be used in search function
    # gs.flatten()
    # print(gs.flatCols)

    # Summary of data
    gs.summary(stat = True)

    # Run the SQL directly (use df instead of table name)
    # gs.showRows(all=True, truncate = False, df = gs.sqlQuery('select Unit from df'))

    # Get the duplicates
    # gs.showRows(df = gs.getDuplicates(count = False, columns='ProductURL'), all = True, truncate=False)

    # Convert file (works with only CSV)
    # gs.convertFile('csv', "|")

    # Compare two datasets
    # gs.compareTwoDatasets('202206220649-Amazon_Category_Jobs.csv', missing = False)
    
    # add remove rows by condition df2=df.filter(~df.provider.unparsed_name.like('%>%')) done in struct tbd array [filter column city ma number aaunu vayena; zip ma text aaunu vayena]
    # compare summary of two datasets with count

    
