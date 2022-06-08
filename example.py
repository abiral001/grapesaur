import os
from modules.gs import GDataAnalysis
from modules.__version__ import __version__

if __name__ == "__main__":
    # Creating the object
    gs = GDataAnalysis(os.getcwd(), increaseMemory=False)

    # Read File : this is a dummy file
    gs.readFile("insight2profit_nrs_world_20220607.csv")

    # Get Column Names
    # print(gs.getColumnNames('networks'))

    # Show Rows
    # gs.showRows(colname = 'facility_name', truncate=False, all=True)

    # Show data count of unique data
    # gs.showUniqueData('Size')

    # Tree view of the dataset
    # gs.tree()

    # Search for a particular data inside the dataset
    # gs.search('B08C1W5N87', searchfield='asin')

    # Get Column names to be used in search function
    # gs.flatten()
    # print(gs.flatCols)

    # Summary of data
    # gs.summary()

    # Run the SQL directly (use df instead of table name)
    # gs.sqlQuery('select * from df')

    # Get the duplicates
    # gs.getDuplicates(count = True)

    # Convert file (works with only CSV)
    # gs.convertFile('csv', "|")

    # Compare two datasets
    # gs.compareTwoDatasets('vericred_christus_health_plan_20220316.json')

    
