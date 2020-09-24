import pandas as pd
import numpy as np
import os
from env import host, user, password

#################### Acquire Mall Customers Data ##################

def get_connection(db, user=user, host=host, password=password):
    '''
    This function uses my info from my env file to
    create a connection url to access the Codeup db.
    '''
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'

def get_telco():
    '''
    This function reads all the Telco data from the CodeUp DB into a DF.
    If telco.csv is not present on the local system it will conduct a DB pull, reads the pull into a DF, 
    and saves the pull as a CSV.
    If telco.csv is present on the local system it will read the csv into a DF
    '''
    filename = "telco.csv"

    if os.path.isfile(filename):
        return pd.read_csv(filename)
    else:
        # read the SQL query into a dataframe
        df = pd.read_sql("""select * from telco_churn.customers left join telco_churn.internet_service_types
        on telco_churn.internet_service_types.internet_service_type_id = telco_churn.customers.internet_service_type_id left join telco_churn.payment_types
        on telco_churn.payment_types.payment_type_id = telco_churn.customers.payment_type_id left join telco_churn.contract_types
        on telco_churn.contract_types.contract_type_id = telco_churn.customers.contract_type_id
        """, get_connection('telco_churn'))

        # Write that dataframe to disk for later. Called "caching" the data for later.
        df.to_csv(filename, index=False)

        # Return the dataframe to the calling code
        return df 

def telco_two_year_charges():
    '''
    This function pulls customer_id, monthly_charages, total_charges, and tenure from CodeUp telco DB 
    where contract_type_id = 3 (two year contracts)
    If telco_two_year_charges.csv is not present on the local system it will conduct a DB pull, 
    reads the pull into a DF, and saves the pull as a CSV.
    If telco.csv is present on the local system it will read the csv into a DF
    '''
    filename = "telco_two_year_charges.csv"

    if os.path.isfile(filename):
        return pd.read_csv(filename)
    else:
        # read the SQL query into a dataframe
        df = pd.read_sql("""select customer_id, monthly_charges, tenure, total_charges from telco_churn.customers
        where contract_type_id = 3""", get_connection('telco_churn'))

        # Write that dataframe to disk for later. Called "caching" the data for later.
        df.to_csv(filename, index=False)

        # Return the dataframe to the calling code
        return df 

def new_mall_data():
    '''
    This function reads the mall customer data from the Codeup db into a df,
    write it to a csv file, and returns the df. 
    '''
    sql_query = 'SELECT * FROM customers'
    df = pd.read_sql(sql_query, get_connection('mall_customers'))
    df.to_csv('mall_customers_df.csv')
    return df

def get_mall_data(cached=False):
    '''
    This function reads in mall customer data from Codeup database if cached == False 
    or if cached == True reads in mall customer df from a csv file, returns df
    '''
    if cached or os.path.isfile('mall_customers_df.csv') == False:
        df = new_mall_data()
    else:
        df = pd.read_csv('mall_customers_df.csv', index_col=0)
    return df

###################### Acquire Titanic Data ######################

    
def new_titanic_data():
    '''
    This function reads the titanic data from the Codeup db into a df,
    write it to a csv file, and returns the df.
    '''
    sql_query = 'SELECT * FROM passengers'
    df = pd.read_sql(sql_query, get_connection('titanic_db'))
    df.to_csv('titanic_df.csv')
    return df

def get_titanic_data(cached=False):
    '''
    This function reads in titanic data from Codeup database if cached == False 
    or if cached == True reads in titanic df from a csv file, returns df
    '''
    if cached or os.path.isfile('titanic_df.csv') == False:
        df = new_titanic_data()
    else:
        df = pd.read_csv('titanic_df.csv', index_col=0)
    return df

###################### Acquire Iris Data ######################

def new_iris_data():
    '''
    This function reads the iris data from the Codeup db into a df,
    writes it to a csv file, and returns the df.
    '''
    sql_query = """
                SELECT species_id,
                species_name,
                sepal_length,
                sepal_width,
                petal_length,
                petal_width
                FROM measurements
                JOIN species
                USING(species_id)
                """
    df = pd.read_sql(sql_query, get_connection('iris_db'))
    df.to_csv('iris_df.csv')
    return df

def get_iris_data(cached=False):
    '''
    This function reads in iris data from Codeup database if cached == False
    or if cached == True reads in iris df from a csv file, returns df
    '''
    if cached or os.path.isfile('iris_df.csv') == False:
        df = new_iris_data()
    else:
        df = pd.read_csv('iris_df.csv', index_col=0)
    return df