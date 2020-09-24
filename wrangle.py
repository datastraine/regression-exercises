import pandas as pd
import numpy as np
import os
import acquire

def wrangle_telco():
    df = acquire.telco_two_year_charges()
    df['total_charges'] = pd.to_numeric(df['total_charges'],errors='coerce')
    df.dropna(inplace = True) 
    return df