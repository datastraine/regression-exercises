import pandas as pd
import numpy as np
import acquire
from sklearn.model_selection import train_test_split
import sklearn.preprocessing

def add_scaled_columns(train, validate, test, scaler, columns_to_scale):
    new_column_names = [c + '_scaled' for c in columns_to_scale]
    scaler.fit(train[columns_to_scale])

    train = pd.concat([
        train,
        pd.DataFrame(scaler.transform(train[columns_to_scale]), columns=new_column_names, index=train.index),
    ], axis=1)
    validate = pd.concat([
        validate,
        pd.DataFrame(scaler.transform(validate[columns_to_scale]), columns=new_column_names, index=validate.index),
    ], axis=1)
    test = pd.concat([
        test,
        pd.DataFrame(scaler.transform(test[columns_to_scale]), columns=new_column_names, index=test.index),
    ], axis=1)
    
    return train, validate, test


def prep_telco():
    telco = acquire.get_telco()
    telco.drop(columns=['contract_type_id.1', 'internet_service_type_id.1', 'payment_type_id.1'], inplace = True)
    telco['total_charges'] = pd.to_numeric(telco['total_charges'],errors='coerce')
    telco.dropna(inplace=True)
    telco['monthly_charges'] = round(telco['total_charges']/telco['tenure'], 2)
    telco['is_alone'] = (telco['dependents'] == 0) & (telco['partner'] == 0)
    telco['tenure_year'] = round(telco['tenure']/12, 2)
    telco['additional_services'] = (telco['tech_support'] == 'Yes') | (telco['online_security'] == 'Yes') | (telco['online_backup'] == 'Yes') | (telco['device_protection'] == 'Yes')
    telco['has_streaming'] = (telco['streaming_tv'] == 'Yes') | (telco['streaming_movies'] == 'Yes')
    telco['has_internet'] = telco['internet_service_type'] != 'None'
    telco['auto_pay'] = telco['payment_type'].str.contains('auto')
    telco['has_fiber'] = telco['internet_service_type'] == 'Fiber'
    telco.replace(False, 0, inplace=True)
    telco.replace(True, 1, inplace=True)
    telco.replace('No', 0, inplace=True)
    telco.replace('Yes', 1, inplace=True)
    telco.replace('No phone service', 0, inplace=True)
    telco.replace('No internet service', 0, inplace=True)
    train_validate, test = train_test_split(telco, test_size=.2, random_state=333, stratify=telco.churn)
    train, validate = train_test_split(train_validate, test_size=.25, random_state=333, stratify=train_validate.churn)
    
    train, validate, test = add_scaled_columns(
    train,
    validate,
    test,
    scaler=sklearn.preprocessing.MinMaxScaler(),
    columns_to_scale=['tenure', 'total_charges', 'monthly_charges', 'tenure_year'])
    
    return train, validate, test