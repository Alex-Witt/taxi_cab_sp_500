import pandas as pd
import numpy as np

def wrangle():
    
    """
    For 2017 - 2019 
    
    
    data: Initial Dataframe
    interim: Single Month Dataframe
    df: final dataframe (concatenated)
    """
    
   
    # Empty Final Dataframe
    df = pd.DataFrame()
    
    # Importing URL's from Text File
    urls = pd.read_csv('./data/to_download.txt', 
                   sep=" ",
                   header=None)
    
    # Loops through URLS. 
    for url in urls.sort_values(by=0)[0].values:
        # Import Data
        data = pd.read_csv(url)

        # Empty Dataframes
        interim = pd.DataFrame()


        ########### Datetime Manipulations ###########

        # Convert to Datetime
        data['tpep_pickup_datetime'] = pd.to_datetime(data['tpep_pickup_datetime'])
        data['tpep_dropoff_datetime'] = pd.to_datetime(data['tpep_dropoff_datetime'])

        # Ride Length
        data['ride_length'] = ((data['tpep_dropoff_datetime'] - data['tpep_pickup_datetime'])
                                .apply(lambda x:x.total_seconds()))
        # Pickup Day for Groupby function
        data['pickup_day'] = data['tpep_pickup_datetime'].map(lambda x:x.strftime('%d'))


        ########### Continuous Variables ###########

        cont_vars = ['passenger_count','trip_distance','fare_amount','extra',
                     'mta_tax','tip_amount','tolls_amount','total_amount','ride_length']


        for calc in ['mean','median','mode']:
            for cont_var in cont_vars:
                # Mean
                if calc == 'mean':
                    interim[cont_var+'_'+calc] = data.groupby('pickup_day')[cont_var].mean()
                # Median
                elif calc == 'median':
                    interim[cont_var+'_'+calc] = data.groupby('pickup_day')[cont_var].median()
                # Mode
                # Pandas Open Source: Please add a Mode Aggregation Function.
                # value_counts is not an adequate replacement for a vectorized function
                else:
                    modes = []
                    for i in np.sort(data['pickup_day'].unique()):
                        mode_test = data.groupby('pickup_day')[cont_var].value_counts()
                        modes.append(mode_test.loc[i].values[0])
                    interim[cont_var+'_'+calc] = modes


        ########### Discrete Variables ###########

        cat_var = ['RatecodeID','store_and_fwd_flag','payment_type','improvement_surcharge']

        for cat in cat_var:
            #print(cat)
            one_hot = pd.get_dummies(data[cat])
            one_hot_columns = one_hot.columns
            one_hot = pd.concat([data['pickup_day'],one_hot], axis=1)

            for column in one_hot_columns:
                interim[cat+'_'+str(column)] = one_hot.groupby('pickup_day')[column].sum()


        ########### Location ID Data ###########


        for loc in ['PULocationID','DOLocationID']:

            first_loc  = []
            second_loc = []
            third_loc  = []
            fourth_loc = []
            fifth_loc  = []

            for day in np.sort(data['pickup_day'].unique()):
                per_day = (data.groupby('pickup_day')[loc]
                           .value_counts()[day]
                           .iloc[0:5]
                           .index
                           .tolist())

                first_loc.append(per_day[0])
                second_loc.append(per_day[1])
                third_loc.append(per_day[2]) 
                fourth_loc.append(per_day[3])
                fifth_loc.append(per_day[4])

            interim[loc+'_'+'1'] = first_loc
            interim[loc+'_'+'2'] = second_loc
            interim[loc+'_'+'3'] = third_loc
            interim[loc+'_'+'4'] = fourth_loc
            interim[loc+'_'+'5'] = fifth_loc

                                                              
        ########### Create Date Column ###########
                                                              
        date = []

        for i in interim.index.values:
            date.append(url[-11:-4]+'-'+i)
        interim['date'] = date
                                                              
        ########### Concatenate Dataframes ###########                                                     

        df = pd.concat([df,interim],axis=0,ignore_index=True, sort=False)
        
        print('##########', url[-11:-4], 'Added ##########')
    
    return df