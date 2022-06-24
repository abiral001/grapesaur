import os
from modules.gs import GDataAnalysis
from modules.__version__ import __version__

if __name__ == "__main__":
    # Creating the object
    gs = GDataAnalysis(os.getcwd(), increaseMemory=True)

    # Read File : this is a dummy file
    gs.readFile("202206241712-Amazon_Category_Jobs_Merged.csv")

    # Get Column Names
    # print(gs.getColumnNames(colname='group_affiliations'))

    # Show Rows
    # gs.showRows(truncate=False, no = 4)

    # Show data count of unique data
    # gs.showUniqueData('group_affiliations.name')

    # Tree view of the dataset
    # gs.tree()

    # Search for a particular data inside the dataset
    # gs.search('null', searchfield='Unit')
    # gs.search('Brandi', searchfield='provider.unparsed_name')

    # Get Column names to be used in search function
    # gs.flatten()
    # print(gs.flatCols)

    # Summary of data
    # gs.summary(stat = True)

    # Run the SQL directly (use df instead of table name)
    # gs.showRows(all=True, truncate = False, df = gs.sqlQuery('select Unit from df'))

    # Get the duplicates
    # gs.showRows(df = gs.getDuplicates(count = False), all = True, truncate=False)

    # Convert file (works with only CSV)
    # gs.convertFile('csv', "|")

    # Compare two datasets
    gs.compareTwoDatasets('202206220649-Amazon_Category_Jobs.csv', missing = False)

    
