import csv

import pandas as pd
import dask
import dask.dataframe as dd

from datetime import datetime

def main():
    transactions_data_all_years = pd.read_csv("transactions.csv")
    transactions_2023_actual = transactions_data_all_years[(transactions_data_all_years['instance_date'].str.endswith('-2023')) &
                                          (transactions_data_all_years['property_usage_en'] == 'Commercial') &
                                          (transactions_data_all_years['trans_group_en'] == 'Sales') &
                                          (transactions_data_all_years['procedure_name_en'] == 'Sell')]
    
    selected_columns = ['transaction_id', 'trans_group_en', 'property_usage_en', 'procedure_name_en', 'instance_date', 'area_id', 'area_name_en', 'procedure_area', 'actual_worth', 'meter_sale_price']
    transactions_selected = transactions_2023_actual[selected_columns]
    
    transactions_selected.to_csv('export_transactions_2023_pandas.csv', index=False, mode='a', quoting=csv.QUOTE_NONNUMERIC, sep=',', quotechar='"')

def dask_main():
    ddf = dd.read_csv("transactions.csv", assume_missing=True)
    filtered_ddf = ddf[(ddf['instance_date'].str.endswith('-2023')) &
                                          (ddf['property_usage_en'] == 'Commercial') &
                                          (ddf['trans_group_en'] == 'Sales') &
                                          (ddf['procedure_name_en'] == 'Sell')]
    selected_columns = ['transaction_id', 'trans_group_en', 'property_usage_en', 'procedure_name_en', 'instance_date', 'area_id', 'area_name_en', 'procedure_area', 'actual_worth', 'meter_sale_price']
    transactions_selected = filtered_ddf[selected_columns]
    with dask.config.set(scheduler='processes'):
        transactions_selected.to_csv('export_transactions_2023_dask.csv', index=False)
    

if __name__=='__main__':
    start1 = datetime.now()
    main()
    print(f'PANDAS TIME : {datetime.now() - start1}')
    start = datetime.now()
    dask_main()
    print(f"DASK TIME : {datetime.now() - start}")
    