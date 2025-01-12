import pandas as pd
import parlament_search_parameters 
import custom_functions

# this function generate CSV file of search parameters for further HTTP requests
search_keys = parlament_search_parameters.proc_extract_parlament_search_parameters()
custom_functions.list_of_dicts_to_csv(search_keys,'search_keys.csv')


# retrieving parlament votes dates using search_parameters
df = pd.read_csv('search_keys.csv')
parlament_ids = df['p_kade_id'].unique()
for p_kade_id in parlament_ids:
    if p_kade_id != 10:
        # print(p_kade_id)
        filtered_df = df.query("p_kade_id == @p_kade_id") #df[df['p_kade_id'].isin(str(p_kade_id))]
        ff = parlament_search_parameters.proc_fetch_vote_results(filtered_df)
        ff.to_csv('votes_{p_kade_id}.csv'.format(p_kade_id=p_kade_id) 
            , quotechar='"'
            ,sep = ','
        )
