import os
from modules.gs import Grapesaur
from modules.__version__ import __version__

if __name__ == "__main__":

    gs = Grapesaur(os.getcwd(), increaseMemory=True)
    gs.readFile("202205131550-amazon_best_sellers_reviews_lumenore_20220513.csv")
    # gs.showRows(1)
    # gs.showRows(colname = 'address_string', truncate=False, all=True)
    # print(gs.getColumnNames('networks'))
    # gs.showUniqueData('specialties.name')
    # gs.search('B08C1W5N87', searchfield='asin')
    gs.summary()
    gs.tree()

    
