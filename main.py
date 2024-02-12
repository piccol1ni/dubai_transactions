import csv
import requests

import pandas as pd

from datetime import datetime
        

class CurrencyChecker():
    
    """ Use api key from site https://app.exchangerate-api.com/keys and use command get_pair_value with naming of currency like USD, AED """
    
    def __init__(self, api_key):
        self.api_key = api_key
        
    def get_last_update(self, currency_1):
        
        """ Check timer of the currency update on site, price actual for this time. """
        
        response = requests.get(f'https://v6.exchangerate-api.com/v6/{self.api_key}/latest/{currency_1}')
        return f'Update time: {response.json()["time_last_update_utc"]}'
    
    def get_pair_value(self, currency_1, currency_2):
        
        """ Return pairy value of currency """
        
        response = requests.get(f'https://v6.exchangerate-api.com/v6/{self.api_key}/latest/{currency_1}')
        return response.json()['conversion_rates'][currency_2]
    
    
class DubaiTransactionsConvertation():
    
    """ Use file name from test task) It must be in this folder.
    Then convert info in new file export_transactions_2023_pandas.csv with columns from test task using Pandas """
    
    def __init__(self, file_name):
        self.file_name = file_name
        
    def convert_info_to_file(self):
        
        
        """ Read and convert file to the actual columns from test task """
        
        transactions_data_all_years = pd.read_csv(self.file_name)
        transactions_2023_actual = transactions_data_all_years[(transactions_data_all_years['instance_date'].str.endswith('-2023')) &
                                          (transactions_data_all_years['property_usage_en'] == 'Commercial') &
                                          (transactions_data_all_years['trans_group_en'] == 'Sales') &
                                          (transactions_data_all_years['procedure_name_en'] == 'Sell')]
    
        selected_columns = ['transaction_id', 'trans_group_en', 'property_usage_en', 'procedure_name_en', 'instance_date', 'area_id', 'area_name_en', 'procedure_area', 'actual_worth', 'meter_sale_price']
        transactions_selected = transactions_2023_actual[selected_columns]
        
        transactions_selected.to_csv('export_transactions_2023_pandas.csv', index=False, mode='a', quoting=csv.QUOTE_NONNUMERIC, sep=',', quotechar='"')
        
class AnalyzeInformation():
    
    
    def __init__(self, file_name):
        self.file_name = file_name
        
    def get_all_sales_2023(self):
        
        """ Return all sales from 2023 year """
        
        sales_info = pd.read_csv(self.file_name)
        return len(sales_info)
        
    def get_all_sales_budget_2023(self):
        
        """ Return all sales budget from 2023 year """
        
        sales_info = pd.read_csv(self.file_name)
        return sales_info['actual_worth'].sum()
    
    def get_information_for_quarter(self, quarter):
        
        """ Return actual information from every quarter """
        
        if quarter == 1:
            mouths = ["01", "02", "03"]
        elif quarter == 2:
            mouths = ["04", "05", "06"]
        elif quarter == 3:
            mouths = ["07", "08", "09"]
        elif quarter == 4:
            mouths = ["10", "11", "12"]
        else:
            return f'Quarter must be in 1, 2, 3, 4'
        
        
        sales_info = pd.read_csv(self.file_name)
        sales_info_for_quarter = sales_info[(sales_info['instance_date'].str.endswith(f'{mouths[0]}-2023')) | sales_info['instance_date'].str.endswith(f'{mouths[1]}-2023') | sales_info['instance_date'].str.endswith(f'{mouths[2]}-2023')]
        full_quarter_info = {
            "total_sales": len(sales_info_for_quarter),
            "total_budget": round(sales_info_for_quarter['actual_worth'].sum(), 2),
            "avarange_sale": round(sales_info_for_quarter['actual_worth'].mean(), 2),
        }
        return full_quarter_info
                
    

if __name__=='__main__':
    start1 = datetime.now()
    create_file = DubaiTransactionsConvertation('transactions.csv')
    create_file.convert_info_to_file()
    print(f'PANDAS TIME : {datetime.now() - start1}')
    
    new_alalyze_2023 = AnalyzeInformation('export_transactions_2023_pandas.csv')

    first_profile = CurrencyChecker('03d5377c7f88c91d9bf86d18')
    
    with open('2023_info.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        field = ["Total Sales 2023", "Total Budget 2023"]
        writer.writerow(field)
        writer.writerow([new_alalyze_2023.get_all_sales_2023(),
                        round(new_alalyze_2023.get_all_sales_budget_2023() * first_profile.get_pair_value('AED', 'USD'), 2)])
        
    
    with open('quarter_info.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        field = ["Quarters", "Quarter I, 2023", "Quarter II, 2023", "Quarter III, 2023", "Quarter IV, 2023"]
        
        writer.writerow(field)
        writer.writerow(["Quantity of sales in commercial property Dubai (UAE)",
                         new_alalyze_2023.get_information_for_quarter(1)['total_sales'], 
                         new_alalyze_2023.get_information_for_quarter(2)['total_sales'], 
                         new_alalyze_2023.get_information_for_quarter(3)['total_sales'], 
                         new_alalyze_2023.get_information_for_quarter(4)['total_sales'],
                         ])
        writer.writerow(["Total sum of sales per quarter 2023",
                         round(new_alalyze_2023.get_information_for_quarter(1)['total_budget'] * first_profile.get_pair_value('AED', 'USD'), 2),
                         round(new_alalyze_2023.get_information_for_quarter(2)['total_budget'] * first_profile.get_pair_value('AED', 'USD'), 2),
                         round(new_alalyze_2023.get_information_for_quarter(3)['total_budget'] * first_profile.get_pair_value('AED', 'USD'), 2),
                         round(new_alalyze_2023.get_information_for_quarter(4)['total_budget'] * first_profile.get_pair_value('AED', 'USD'), 2),
                         ])
        writer.writerow(["Median sum of sales per quarter 2023",
                         round(new_alalyze_2023.get_information_for_quarter(1)['avarange_sale'] * first_profile.get_pair_value('AED', 'USD'), 2),
                         round(new_alalyze_2023.get_information_for_quarter(2)['avarange_sale'] * first_profile.get_pair_value('AED', 'USD'), 2),
                         round(new_alalyze_2023.get_information_for_quarter(3)['avarange_sale'] * first_profile.get_pair_value('AED', 'USD'), 2),
                         round(new_alalyze_2023.get_information_for_quarter(4)['avarange_sale'] * first_profile.get_pair_value('AED', 'USD'), 2),
                         ])