import sys
import pandas as pd
import numpy as np
import pyodbc
import sqlalchemy as sa
from statsmodels.robust.scale import mad


# Create DB connection variables #
server = 'PRDITSMSRDSSH22\JAMESSQL'
db = 'BLUETOOTH'
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
engine = sa.create_engine('mssql+pyodbc://PRDITSMSRDSSH22\\JAMESSQL/BLUETOOTH?driver=SQL+Server+Native+Client+11.0')
sql = """SELECT TOP 1000 id, deviceid, areaid, LEADAREA, SUBT_DC, SUBSTRING(CONVERT(nvarchar(6),entered_time,112),1,6) entered, entered_time,
TT_Alg_Flag FROM TT_Alg_1_0"""
cursor = conn.cursor()
##################################

cursor.execute("SELECT COUNT(*) FROM BLUETOOTH_DETAILS")    # Get data count for the route
(count,) = cursor.fetchone()    # Assign value to variable "count"
df = pd.read_sql(sql, conn)     # Create data frame using Pandas

df.columns = ['id', 'deviceid', 'areaid', 'next_areaid', 'time', 'entered', 'entered_time', 'flag']  # Rename column names

var = ['x','10175','10508','10439','10443']     # For testing input argv
dfnew = pd.DataFrame()          # Create a new and empty data frame for storing filtered data



global counter
counter = 0

mins = [10, 20, 30]     # Create a list of time intervals
lab = ['madflag10', 'madflag20', 'madflag30']   # Create a list of time interval lables

# Using loop and if conditions to identify the beginning, in-between and last nodes,
# so invalid data rows (e.g. devices appearing in first node and then in last node) can be filtered out
for i in range(1, 5):
    dftemp = pd.DataFrame()     # Create temporary data frame
    dftemp.drop(dftemp.index, inplace=True)     # Clear data frame before every iteration

    if (i==1):      # Beginning node
        ### To be functionized
        dftemp = df[((df.areaid == var[i]) & (df.next_areaid == var[i + 1]))]   # Filter data rows
        dftemp = dftemp.sort_values(['areaid', 'next_areaid', 'entered_time'])    # Sort data by columns
        dftemp = dftemp.reset_index(drop=True)  # Reset index for sorted data rows
        ######################

        ### To be functionized
        for index, row in enumerate(dftemp.itertuples(), 0):    # Iterate through each row in temporary data frame
            #############
            #print('index: ' + row.id + ' ' + str(row.entered_time) + ' ' + str(row.time))
            #print(counter)
            counter += 1
            #############
            uptime = []     # Create a list for upper bound of time
            bottime = []    # Create a list for lower bound of time
            medlag = []     # Create a list for median of previous data
            medlead = []    # Create a list for median of following data
            madlag = []     # Create a list for MAD of previous data
            madlead = []    # Create a list for MAD of following data

            ### Use for loop to get upper and lower bounds of time intervals
            for j in range(0, 3):
                uptime.append(row.entered_time + pd.Timedelta(minutes=mins[j]))     # Get upper bound of time interval
                bottime.append(row.entered_time - pd.Timedelta(minutes=mins[j]))    # Get lower bound of time interval
            ###

            # Extract previous data within the upper bound of time interval and time less than 1 hour
            dflag1 = dftemp[(dftemp['entered_time'] > bottime[0]) & (dftemp['entered_time'] < row.entered_time) & (dftemp['time'] < 60)]
            # Extract following data within the lower bound of time interval and time less than 1 hour
            dflead1 = dftemp[(dftemp['entered_time'] < uptime[0]) & (dftemp['entered_time'] > row.entered_time) & (dftemp['time'] < 60)]
            # & (dfnew['areaid'] == row['areaid']) & (dfnew['next_areaid'] == row['next_areaid'])]
            dflag2 = dftemp[(dftemp['entered_time'] > bottime[1]) & (dftemp['entered_time'] < row.entered_time) & (dftemp['time'] < 60)]
            dflead2 = dftemp[(dftemp['entered_time'] < uptime[1]) & (dftemp['entered_time'] > row.entered_time) & (dftemp['time'] < 60)]
            dflag3 = dftemp[(dftemp['entered_time'] > bottime[2]) & (dftemp['entered_time'] < row.entered_time) & (dftemp['time'] < 60)]
            dflead3 = dftemp[(dftemp['entered_time'] < uptime[2]) & (dftemp['entered_time'] > row.entered_time) & (dftemp['time'] < 60)]

            ### Use for loop to get medians and MAD of different time intervals of previous and following data
            for d in (dflag1, dflag2, dflag3):
                medlag.append(d['time'].median())
                madlag.append(mad(d['time']))

            for d in (dflead1, dflead2, dflead3):
                medlead.append(d['time'].median())
                madlead.append(mad(d['time']))
            ###

            ### Data row check
            if (row.id == '77884099'):
                print('id: ' + row.id)
                print('time: ' + str(row.time))
                print('madlag10')
                print(str(medlag[2])+' '+ str(madlag[2]))
                print(dflag3)

                print(str(medlead[2])+' '+ str(madlead[2]))
                print(dflead3)
            ###
    
            if (row.time > 60):     # Directly assign 0 to madflag if the row has time more than 1 hour
                for l in lab:       # Use for loop to assign 3 different time intervals
                        dftemp.at[index, l] = '0'   

            else:       # Assign madflag depending whether the time of the row is within the range of
                        # median+-MAD*2 in either one of the previous and following dataset'
                for k in range(0, 3):   # Use for loop to assign 3 different time intervals
                    dftemp.at[index, lab[k]] = np.where(((row.time >= (medlag[k]-madlag[k]*2)) & (row.time <= (medlag[k]+madlag[k]*2))) |
                                                        ((row.time >= (medlead[k]-madlead[k]*2)) & (row.time <= (medlead[k]+madlead[k]*2)))
                                                        , '1', '0')

        dfnew = dfnew.append(dftemp)    # Append result to summary data frame
        
    elif (i == 4):  # Last node
        dftemp = df[((df.areaid == var[i]) & (df.next_areaid == var[i - 1]))]  
        dftemp = dftemp.sort_values(['areaid', 'next_areaid', 'entered_time'])  
        dftemp = dftemp.reset_index(drop=True)  


        for index, row in enumerate(dftemp.itertuples(), 0): 
            uptime = []
            bottime = []
            medlag = []
            medlead = []
            madlag = []
            madlead = []
            lab = ['madflag10', 'madflag20', 'madflag30']
            for j in range(0, 3):
                uptime.append(row.entered_time + pd.Timedelta(minutes=mins[j])) 
                bottime.append(row.entered_time - pd.Timedelta(minutes=mins[j]))  

            dflag1 = dftemp[(dftemp['entered_time'] > bottime[0]) & (dftemp['entered_time'] < row.entered_time) & (
                        dftemp['time'] < 60)]
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
                    dftemp.at[index, l] = '0' 
            else: 
                for k in range(0, 3):
                    dftemp.at[index, lab[k]] = np.where(
                        ((row.time >= (medlag[k] - madlag[k] * 2)) & (row.time <= (medlag[k] + madlag[k] * 2))) |
                        ((row.time >= (medlead[k] - madlead[k] * 2)) & (row.time <= (medlead[k] + madlead[k] * 2)))
                        , '1', '0')
        dfnew = dfnew.append(dftemp)  
		
    else:  # Middle node
        dftemp = df[(df.areaid == var[i]) & (
                    (df.next_areaid == var[i + 1]) | (df.next_areaid == var[i - 1]))]  
        dftemp = dftemp.sort_values(['areaid', 'next_areaid', 'entered_time'])  
        dftemp = dftemp.reset_index(drop=True)  

        for index, row in enumerate(dftemp.itertuples(), 0):  
            uptime = []
            bottime = []
            medlag = []
            medlead = []
            madlag = []
            madlead = []
            lab = ['madflag10', 'madflag20', 'madflag30']
			
            for j in range(0, 3):
                uptime.append(row.entered_time + pd.Timedelta(minutes=mins[j]))
                bottime.append(row.entered_time - pd.Timedelta(minutes=mins[j]))

            dflag1 = dftemp[(dftemp['entered_time'] > bottime[0]) & (dftemp['entered_time'] < row.entered_time) & (
                        dftemp['time'] < 60)]
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
                    dftemp.at[index, l] = '0'  
            else:  
                for k in range(0, 3):
                    dftemp.at[index, lab[k]] = np.where(
                        ((row.time >= (medlag[k] - madlag[k] * 2)) & (row.time <= (medlag[k] + madlag[k] * 2))) |
                        ((row.time >= (medlead[k] - madlead[k] * 2)) & (row.time <= (medlead[k] + madlead[k] * 2)))
                        , '1', '0')
						
        dfnew = dfnew.append(dftemp)  

###################################################################################
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
conn.commit()                                   # Bulk insert into target table in DB