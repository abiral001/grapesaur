import os
from modules.gs import Grapesaur
from modules.__version__ import __version__

if __name__ == "__main__":

    gs = Grapesaur(os.getcwd(), increaseMemory=True)
    gs.readFile("vericred_bs_ca_promise_new_20220530.json")
    #gs.compareTwoDatasets('vericred_christus_health_plan_20220316.json')
    # gs.removeDuplicates()
    # gs.showRows(1)
    gs.showRows(colname = 'facility_name', truncate=False, all=True)
    # print(gs.getColumnNames('networks'))
    # gs.showUniqueData('specialties.name')
    # gs.search('B08C1W5N87', searchfield='asin')
    #gs.summary()
    #gs.tree()

    
