import sys
import pandas as pd
import numpy as np
import pyodbc
import sqlalchemy as sa
from statsmodels.robust.scale import mad
import time
import datetime

start = time.time()     # Get start time of the data cleanup

### Create DB connection variables ####
server = 'PRDITSMSRDSSH22\JAMESSQL'
db = 'BLUETOOTH'
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
engine = sa.create_engine('mssql+pyodbc://PRDITSMSRDSSH22\\JAMESSQL/BLUETOOTH?driver=SQL+Server+Native+Client+11.0')
sql = """SELECT id, deviceid, areaid, LEADAREA, SUBT_DC, SUBSTRING(CONVERT(nvarchar(6),entered_time,112),1,6) entered, entered_time,
TT_Alg_Flag, suburb, st1, st2, X, Y FROM TT_Alg_1_0"""
cursor = conn.cursor()
#######################################

### Main operation part ###
df = pd.read_sql(sql, conn)     # Create data frame using Pandas
df.columns = ['id', 'deviceid', 'areaid', 'next_areaid', 'time', 'entered', 'entered_time', 'flag', 'suburb', 'street1', 'street2', 'longtitude', 'latitude']  # Rename column names
dfnew = pd.DataFrame()          # Create a new and empty data frame for storing filtered data

global counter
counter = 0     # Create a counter for checking progress

mins = [10, 20, 30]     # Create a list of time intervals
lab = ['madflag10', 'madflag20', 'madflag30']   # Create a list of time interval lables
level = [[0.4, 0.3, 0.3], [0.3, 0.4, 0.4], [0.1, 0.2, 0.7]]     # Create a list for 3 levels of weights (Less busy, busy and very busy roads)

# Using loop and if conditions to identify the beginning, in-between and last nodes,
# so invalid data rows (e.g. devices appearing in first node and then in last node) can be filtered out
for i in range(1, len(sys.argv)-1):
    dftemp = pd.DataFrame()     # Create temporary data frame
    dftemp.drop(dftemp.index, inplace=True)     # Clear data frame before every iteration

    if (i==1):      # Beginning node
        ### Repetitive, to be functionized ###
        dftemp = df[((df.areaid == sys.argv[i]) & (df.next_areaid == sys.argv[i + 1]))]   # Filter data rows
        dftemp = dftemp.sort_values(['areaid', 'next_areaid', 'entered_time'])    # Sort data by columns
        dftemp = dftemp.reset_index(drop=True)  # Reset index for sorted data rows
        ######################################

        ### Repetitive, to be functionized ###
        for index, row in enumerate(dftemp.itertuples(), 0):    # Iterate through each row in temporary data frame
            print(counter)
            counter += 1    # Display counter

            uptime = []     # Create a list for upper bound of time
            bottime = []    # Create a list for lower bound of time
            medlag = []     # Create a list for median of previous data
            medlead = []    # Create a list for median of following data
            madlag = []     # Create a list for MAD of previous data
            madlead = []    # Create a list for MAD of following data

            ### Use for loop to get upper and lower bounds of time intervals, to be functionized ###
            for j in range(0, 3):
                uptime.append(row.entered_time + pd.Timedelta(minutes=mins[j]))     # Get upper bound of time interval
                bottime.append(row.entered_time - pd.Timedelta(minutes=mins[j]))    # Get lower bound of time interval
            ########################################################################################

            # Extract previous data within the upper bound of time interval and time less than 1 hour for 3 different time intervals
            dflag1 = dftemp[(dftemp['entered_time'] > bottime[0]) & (dftemp['entered_time'] < row.entered_time) & (dftemp['time'] < 60)]
            # Extract following data within the lower bound of time interval and time less than 1 hour for 3 different time intervals
            dflead1 = dftemp[(dftemp['entered_time'] < uptime[0]) & (dftemp['entered_time'] > row.entered_time) & (dftemp['time'] < 60)]
            dflag2 = dftemp[(dftemp['entered_time'] > bottime[1]) & (dftemp['entered_time'] < row.entered_time) & (dftemp['time'] < 60)]
            dflead2 = dftemp[(dftemp['entered_time'] < uptime[1]) & (dftemp['entered_time'] > row.entered_time) & (dftemp['time'] < 60)]
            dflag3 = dftemp[(dftemp['entered_time'] > bottime[2]) & (dftemp['entered_time'] < row.entered_time) & (dftemp['time'] < 60)]
            dflead3 = dftemp[(dftemp['entered_time'] < uptime[2]) & (dftemp['entered_time'] > row.entered_time) & (dftemp['time'] < 60)]

            ### Use for loop to get medians and MAD of different time intervals of previous and following data, to be funtionized ###
            for d in (dflag1, dflag2, dflag3):
                medlag.append(d['time'].median())
                madlag.append(mad(d['time']))

            for d in (dflead1, dflead2, dflead3):
                medlead.append(d['time'].median())
                madlead.append(mad(d['time']))
            #########################################################################################################################

            ### Fill the MAD flags and final weighted flag 'wflag' ###
            if (row.time > 60):     # Directly assign 0 to madflag if the row has time more than 1 hour
                for l in lab:       # Use for loop to assign 3 different time intervals
                        dftemp.at[index, l] = '0'
            else:       # Assign madflag depending whether the time of the row is within the range of
                        # median+-MAD*2 in either one of the previous and following dataset'
                for k in range(0, 3):   # Use for loop to assign 3 different time intervals
                    if (madlag[k] == 0) & (madlead[k] == 0):    # Exclude extreme case of both previous and following MAD are both 0
                        dftemp.at[index, lab[k]] = np.where(
                            ((row.time >= (medlag[k] - 2)) & (row.time <= (medlag[k] + 2))) |
                            ((row.time >= (medlead[k] - 2)) & (row.time <= (medlead[k] + 2)))
                            , '1', '0')     # Under this case, use the range of median-2 and median+2 to identify valid data
                    else:
                        dftemp.at[index, lab[k]] = np.where(((row.time >= (medlag[k]-madlag[k]*2)) & (row.time <= (medlag[k]+madlag[k]*2))) |
                                                            ((row.time >= (medlead[k]-madlead[k]*2)) & (row.time <= (medlead[k]+madlead[k]*2)))
                                                            , '1', '0')     # Normal case, use the range of median-2*MAD and
                                                                            # median+2*MAD to identify valid data

            a = [int(x) for x in list((dftemp.loc[index, 'madflag10':'madflag30']))]    # Create a list and convert MAD flags to integers
            dftemp.at[index, 'wflag'] = np.where(((sum(np.multiply(level[int(sys.argv[len(sys.argv)-1])-1], a))) >= 0.6), '1', '0')
            # Calculate the weighted sum and if greater than 0.6, mark wflag 1
            ##########################################################

        ######################################
        dfnew = dfnew.append(dftemp)    # Append result to summary data frame
    # Beginning node part ends


    elif (i == len(sys.argv)-2):  # Last node, repeat structure of beginning node, same parts need to be functionized
        dftemp = df[((df.areaid == sys.argv[i]) & (df.next_areaid == sys.argv[i - 1]))]  # Filter data rows
        dftemp = dftemp.sort_values(['areaid', 'next_areaid', 'entered_time'])  # Sort data by columns
        dftemp = dftemp.reset_index(drop=True)  # Reset index for sorted data rows

        ### To be functionized
        for index, row in enumerate(dftemp.itertuples(), 0):  # Iterate through each row in temporary data frame
            print(counter)
            counter += 1

            uptime = []
            bottime = []
            medlag = []
            medlead = []
            madlag = []
            madlead = []

            for j in range(0, 3):
                uptime.append(row.entered_time + pd.Timedelta(minutes=mins[j]))  # Get upper bound of time interval
                bottime.append(row.entered_time - pd.Timedelta(minutes=mins[j]))  # Get lower bound of time interval

            # Extract previous data within the upper bound of time interval and time less than 1 hour for 3 different time intervals
            dflag1 = dftemp[(dftemp['entered_time'] > bottime[0]) & (dftemp['entered_time'] < row.entered_time) & (
                        dftemp['time'] < 60)]
            # Extract following data within the lower bound of time interval and time less than 1 hour for 3 different time intervals
            dflead1 = dftemp[(dftemp['entered_time'] < uptime[0]) & (dftemp['entered_time'] > row.entered_time) & (
                        dftemp['time'] < 60)]
            dflag2 = dftemp[(dftemp['entered_time'] > bottime[1]) & (dftemp['entered_time'] < row.entered_time) & (
                        dftemp['time'] < 60)]
            dflead2 = dftemp[(dftemp['entered_time'] < uptime[1]) & (dftemp['entered_time'] > row.entered_time) & (
                        dftemp['time'] < 60)]
            dflag3 = dftemp[(dftemp['entered_time'] > bottime[2]) & (dftemp['entered_time'] < row.entered_time) & (
                        dftemp['time'] < 60)]
            dflead3 = dftemp[(dftemp['entered_time'] < uptime[2]) & (dftemp['entered_time'] > row.entered_time) & (
                        dftemp['time'] < 60)]

            for d in (dflag1, dflag2, dflag3):
                medlag.append(d['time'].median())
                madlag.append(mad(d['time']))
            for d in (dflead1, dflead2, dflead3):
                medlead.append(d['time'].median())
                madlead.append(mad(d['time']))

            if (row.time > 60):
                for l in lab:
                    dftemp.at[index, l] = '0'  # Directly assign 0 to madflag if the row has time more than 1 hour
            else:  # Assign madflag depending whether the time of the row is within the range of
                # median+-MAD*2 in either one of the previous and following dataset'
                for k in range(0, 3):
                    if (madlag[k] == 0) & (madlead[k] == 0):
                        dftemp.at[index, lab[k]] = np.where(
                            ((row.time >= (medlag[k] - 2)) & (row.time <= (medlag[k] + 2))) |
                            ((row.time >= (medlead[k] - 2)) & (row.time <= (medlead[k] + 2)))
                            , '1', '0')
                    else:
                        dftemp.at[index, lab[k]] = np.where(
                            ((row.time >= (medlag[k] - madlag[k] * 2)) & (row.time <= (medlag[k] + madlag[k] * 2))) |
                            ((row.time >= (medlead[k] - madlead[k] * 2)) & (row.time <= (medlead[k] + madlead[k] * 2)))
                            , '1', '0')
            a = [int(x) for x in list((dftemp.loc[index, 'madflag10':'madflag30']))]
            dftemp.at[index, 'wflag'] = np.where(((sum(np.multiply(level[int(sys.argv[len(sys.argv)-1])-1], a))) >= 0.6), '1', '0')
        dfnew = dfnew.append(dftemp)  # Append result to summary data frame
    # Last node part ends

    else:  # Middle node, repeat structure of beginning node(except the additional for loop), same parts need to be functionized

        dire = [sys.argv[i+1], sys.argv[i-1]]   # Create a list for previous and next nodes to the middle node

        for j in range(0, 2):   # Use a loop to calculate the case of the previous and next node
            dftemp = df[(df.areaid == sys.argv[i]) & (df.next_areaid == dire[j])]  # Filter data rows from dire list
            dftemp = dftemp.sort_values(['areaid', 'next_areaid', 'entered_time'])  # Sort data by columns
            dftemp = dftemp.reset_index(drop=True)  # Reset index for sorted data rows

            for index, row in enumerate(dftemp.itertuples(), 0):  # Iterate through each row in temporary data frame
                print(counter)
                counter += 1

                uptime = []
                bottime = []
                medlag = []
                medlead = []
                madlag = []
                madlead = []

                for j in range(0, 3):
                    uptime.append(row.entered_time + pd.Timedelta(minutes=mins[j]))  # Get upper bound of time interval
                    bottime.append(row.entered_time - pd.Timedelta(minutes=mins[j]))  # Get lower bound of time interval

                # Extract previous data within the upper bound of time interval and time less than 1 hour for 3 different time intervals
                dflag1 = dftemp[(dftemp['entered_time'] > bottime[0]) & (dftemp['entered_time'] < row.entered_time) & (
                            dftemp['time'] < 60)]
                # Extract following data within the lower bound of time interval and time less than 1 hour for 3 different time intervals
                dflead1 = dftemp[(dftemp['entered_time'] < uptime[0]) & (dftemp['entered_time'] > row.entered_time) & (
                            dftemp['time'] < 60)]
                dflag2 = dftemp[(dftemp['entered_time'] > bottime[1]) & (dftemp['entered_time'] < row.entered_time) & (
                            dftemp['time'] < 60)]
                dflead2 = dftemp[(dftemp['entered_time'] < uptime[1]) & (dftemp['entered_time'] > row.entered_time) & (
                            dftemp['time'] < 60)]
                dflag3 = dftemp[(dftemp['entered_time'] > bottime[2]) & (dftemp['entered_time'] < row.entered_time) & (
                            dftemp['time'] < 60)]
                dflead3 = dftemp[(dftemp['entered_time'] < uptime[2]) & (dftemp['entered_time'] > row.entered_time) & (
                            dftemp['time'] < 60)]

                for d in (dflag1, dflag2, dflag3):
                    medlag.append(d['time'].median())
                    madlag.append(mad(d['time']))
                for d in (dflead1, dflead2, dflead3):
                    medlead.append(d['time'].median())
                    madlead.append(mad(d['time']))

                if (row.time > 60):
                    for l in lab:
                        dftemp.at[index, l] = '0'  # Directly assign 0 to madflag if the row has time more than 1 hour
                else:  # Assign madflag depending whether the time of the row is within the range of
                    # median+-MAD*2 in either one of the previous and following dataset'
                    for k in range(0, 3):
                        if (madlag[k] == 0) & (madlead[k] == 0):
                            dftemp.at[index, lab[k]] = np.where(((row.time >= (medlag[k] - 2)) & (row.time <= (medlag[k] + 2))) |
                                                                ((row.time >= (medlead[k] - 2)) & (row.time <= (medlead[k] + 2)))
                                                                , '1', '0')
                        else:
                            dftemp.at[index, lab[k]] = np.where(
                                ((row.time >= (medlag[k] - madlag[k] * 2)) & (row.time <= (medlag[k] + madlag[k] * 2))) |
                                ((row.time >= (medlead[k] - madlead[k] * 2)) & (row.time <= (medlead[k] + madlead[k] * 2)))
                                , '1', '0')
                a = [int(x) for x in list((dftemp.loc[index, 'madflag10':'madflag30']))]
                dftemp.at[index, 'wflag'] = np.where(((sum(np.multiply(level[int(sys.argv[len(sys.argv)-1])-1], a))) >= 0.6), '1', '0')
            dfnew = dfnew.append(dftemp)  # Append result to summary data frame
    # Middle node part ends
### End of main operation ###
###################################################################################



### Reorder columns of the data frame and import data into MSSQL DB ###
sequence = ['id', 'deviceid', 'areaid', 'next_areaid', 'time', 'entered', 'entered_time', 'flag', 'madflag10', 'madflag20',
            'madflag30', 'wflag', 'suburb', 'street1', 'street2', 'longtitude', 'latitude']     # Create sequence list
dfnew = dfnew.reindex(columns = sequence)    # Reorder columns according to sequence list
#######################################################################

dfnew.to_csv("E:/James/outnew.csv", index=False, encoding='utf-8')     # Output valid data to temporary csv file

cursor.execute('TRUNCATE TABLE TT_Alg_1_0_1')   # Truncate target table in DB
cursor.execute("""
                BULK INSERT TT_Alg_1_0_1
                FROM 'E:/James/outnew.csv'
                WITH
                (
                    CODEPAGE = 'RAW',
                    FIRSTROW = 2,
                    FIELDTERMINATOR = ','
                );""")
conn.commit()                                   # Bulk insert into target table in DB from temp csv file
#######################################################################

### Log file creation ###
end = time.time()   # Get end time
time = (end-start)  # Elasped time
with open("E:/James/log.txt", "w") as text:     # Set file path and create log file
    print((datetime.datetime.fromtimestamp(start).strftime('%Y-%m-%d %H:%M:%S')), file=text)    # Start time of the data cleanup
    print((datetime.datetime.fromtimestamp(end).strftime('%Y-%m-%d %H:%M:%S')), file=text)      # End time of the data cleanup
    print(str(int(round(time/3600)))+' hrs '+ str(int(round((time/60)%60)))+' mins '+ str(round(((time%60)%60), 2))+' secs', file=text) # Elasped time in hh:mm:ss
    print('wflag 1 count: ' + str(counter), file=text)  # The number of valid data for traveling time calculation
########################

### Calculate mean and median for valid data and import to another table in DB ###
dfaggr = pd.DataFrame()     # Create an aggregate data frame

dfaggr = dfnew[dfnew['wflag'] == '1'].groupby(['areaid', 'next_areaid'], as_index=False)['time'].aggregate('mean').round(3)
# Calculate mean for data having wflag = '1', which are valid data
temp = dfnew[dfnew['wflag'] == '1'].groupby(['areaid', 'next_areaid'], as_index=False)['time'].aggregate('median')
dfaggr['median'] = temp['time']
# Calculate median for data having wflag = '1', which are valid data

dfaggr.columns = ['areaid', 'next_areaid', 'mean', 'median']    # Rename headers
dfaggr.to_sql(name='TT_Aggr_1', con=engine, if_exists='replace', index=False)   # Insert into DB
##################################################################################
