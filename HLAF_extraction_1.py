
# coding: utf-8

# In[1]:


import os
import pandas as pd
import numpy as np
import sys


# In[2]:


timeseries_file = sys.argv[1]


# In[3]:


# Create a function called "chunks" with two arguments, l and n:
def chunks(l, n):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i+n]


# In[4]:


#Counts the number of bouts by counting the number of unbroken epochs
def numberOfBoutsForSeries(activity_series):
    bout_counter = 0

    for index, val in activity_series.iteritems():
        if index == 0 and val == 1:
            bout_counter = bout_counter + 1

        if index < (len(activity_series) - 1) and val == 0 and activity_series[index + 1] == 1:
            bout_counter = bout_counter + 1

    return bout_counter


# In[5]:


#This function calculates the average length for bouts in each activity segment calculating the length of each bout
#and then dividing it by the number of bouts in the segment
def averageBoutLengthForSeries(activity_series):
    count = 0
    for value in activity_series:
        if value == 1:
            count = count + 1
    if numberOfBoutsForSeries(activity_series) > 0:
        average = float(count / numberOfBoutsForSeries(activity_series))
        return average
    else:
        return 0


# In[6]:


#This function calculates the percentage time spent for the activity segment passed in the arguments by
# counting the epochs and then dividing it by the total activity segment length
def percentageTimeSpentForSeries(activity_series):

    count = 0
    for value in activity_series:
        if value == 1:
            count = count + 1
            
    percentage = (count / len(activity_series)) * 100
    return percentage


# In[7]:


def averageSleepTimes(timeseries):

    days_blocks = list(chunks(timeseries["sleep"], 2880))
    day = 0
    sleep_start_times = []
    sleep_end_times = []
    for days in days_blocks:
        day = day + 1
        print("Day " + str(day))
        bout_count = 0

        days_list = list(days)
        all_bouts_for_day_df = pd.DataFrame(columns=["Bout count", "Bout begin", "Bout end", "Bout length"])

        for idx, val in enumerate(days_list):

            #Beginning of bout
            if idx < (len(days_list) - 1) and val == 0 and days_list[idx + 1] == 1:
               
                bout_begin = idx + 1

            #Beginning of bout
            if idx ==0 and val == 1 and days_list[idx + 1] == 1:
               
                bout_begin = idx

	    #Beginning of bout
            if idx ==0 and val == 1 and days_list[idx + 1] == 0:
               
                bout_begin = idx

            #End of bout
            if idx < (len(days_list) - 1) and val == 1 and days_list[idx + 1] == 0:
                
                bout_end = idx + 1
                bout_length = bout_end - bout_begin

                bout_count = bout_count + 1
                bout_detail = [bout_count, bout_begin, bout_end, bout_length]
                all_bouts_for_day_df.loc[len(all_bouts_for_day_df)] = bout_detail

            if idx == (len(days_list) - 1) and val==1:
               
                bout_end = idx
                bout_length = bout_end - bout_begin

                bout_count = bout_count + 1
                bout_detail = [bout_count, bout_begin, bout_end, bout_length]
                all_bouts_for_day_df.loc[len(all_bouts_for_day_df)] = bout_detail
            
            if idx == 0 and val == 1 and days_list[idx + 1] == 0:
                bout_begin = 0
                bout_end = idx + 1
                bout_length = bout_end - bout_begin

                bout_count = bout_count + 1
                bout_detail = [bout_count, bout_begin, bout_end, bout_length]
                all_bouts_for_day_df.loc[len(all_bouts_for_day_df)] = bout_detail
                
        
        if len(all_bouts_for_day_df) > 1:
            longest_bout = all_bouts_for_day_df.loc[pd.Series.idxmax(all_bouts_for_day_df["Bout length"].astype(float))]
            sleep_start_times.append(longest_bout['Bout begin'])
            sleep_end_times.append(longest_bout['Bout end'])
        if len(all_bouts_for_day_df) == 1:
            sleep_start_times.append(all_bouts_for_day_df.iloc[0]["Bout begin"])
            sleep_end_times.append(all_bouts_for_day_df.iloc[0]["Bout end"])
        if all_bouts_for_day_df.empty:
            sleep_start_times.append(0)
            sleep_end_times.append(0)
    print(sleep_start_times)
    print(sleep_end_times)
    average_sleep_start = (int(sum(sleep_start_times) / len(sleep_start_times)))
    average_sleep_end = (int(sum(sleep_end_times) / len(sleep_end_times)))
        
    return [average_sleep_start, average_sleep_end]

# In[9]:


column_titles = ["id",
                 "percent time walking afternoon",
                 "percent time walking evening",
                 "percent time walking overnight",
                 "percent time walking morning",
                 "avg bout length walking afternoon",
                 "avg bout length walking evening",
                 "avg bout length walking overnight",
                 "avg bout length walking morning",
                 "avg number bout walking afternoon",
                 "avg number bout walking evening",
                 "avg number bout walking overnight",
                 "avg number bout walking morning",
                 "percent time mod afternoon",
                 "percent time mod evening",
                 "percent time mod overnight",
                 "percent time mod morning",
                 "avg bout length mod afternoon",
                 "avg bout length mod evening",
                 "avg bout length mod overnight",
                 "avg bout length mod morning",
                 "avg number bout mod afternoon",
                 "avg number bout mod evening",
                 "avg number bout mod overnight",
                 "avg number bout mod morning",
                 "percent time light tasks afternoon",
                 "percent time light tasks evening",
                 "percent time light tasks overnight",
                 "percent time light tasks morning",
                 "avg bout length light tasks afternoon",
                 "avg bout length light tasks evening",
                 "avg bout length light tasks overnight",
                 "avg bout length light tasks morning",
                 "avg number bout light tasks afternoon",
                 "avg number bout light tasks evening",
                 "avg number bout light tasks overnight",
                 "avg number bout light tasks morning",
                'percent time sed afternoon',
                 'percent time sed evening',
                 'percent time sed overnight',
                 'percent time sed morning',
                 'avg bout length sed afternoon',
                 'avg bout length sed evening',
                 'avg bout length sed afternoon',
                 'avg bout length sed morning',
                 'avg number bout sed afternoon',
                 'avg number bout sed evening',
                 'avg number bout sed overnight',
                 'avg number bout sed morning',
                 'percent time sleep afternoon',
                 'percent time sleep evening',
                 'percent time sleep overnight',
                 'percent time sleep morning',
                 'avg bout length sleep afternoon',
                 'avg bout length sleep evening',
                 'avg bout length sleep afternoon',
                 'avg bout length sleep morning',
                 'avg number bout sleep afternoon',
                 'avg number bout sleep evening',
                 'avg number bout sleep overnight',
                 'avg number bout sleep morning']
activity_per_tod = pd.DataFrame(columns=column_titles)
activity_per_tod


# In[10]:


activity_classes = ["walking", "moderate", "tasks-light", "sedentary", "sleep"]


# In[11]:


activity_features_per_person = []
parts = timeseries_file.split("_")
activity_features_per_person.append(parts[2])

timeSeries = pd.read_csv(timeseries_file)
timeSeries.fillna(axis=0, inplace=True, value=0)

avg_sleep_start_end_times = averageSleepTimes(timeSeries)

print('Average sleep start time: '  + str(avg_sleep_start_end_times[0]))
print('Average sleep end time: ' + str(avg_sleep_start_end_times[1]))

if len(timeSeries.index) == 20159:
    for activity_class in activity_classes:

        activity_series = timeSeries[activity_class]
        activity_by_day = list(chunks(activity_series, 2880)) #Break time series in days

        afternoon_avgs_percent = []     #10am  - 4pm
        evening_avgs_percent = []       #4pm   - 10pm
        overnight_avgs_percent = []     #10pm  - 4am
        morning_avgs_percent = []       #4am   - 10am

        afternoon_avgs_boutlength = []     #10am  - 4pm
        evening_avgs_boutlength = []       #4pm   - 10pm
        overnight_avgs_boutlength = []     #10pm  - 4am
        morning_avgs_boutlength = []       #4am   - 10am

        afternoon_avgs_boutnumber = []     #10am  - 4pm
        evening_avgs_boutnumber = []       #4pm   - 10pm
        overnight_avgs_boutnumber = []     #10pm  - 4am
        morning_avgs_boutnumber = []       #4am   - 10am

        for day_block in activity_by_day:

            sleep_segment = day_block[avg_sleep_start_end_times[0] : avg_sleep_start_end_times[1]]

            activity_blocks = day_block.drop(day_block.index[range(avg_sleep_start_end_times[0], avg_sleep_start_end_times[0])], axis=0)
            activity_segment_length = int(len(activity_blocks) / 3)

            activity_by_day_intervals = list(chunks(activity_blocks, activity_segment_length))
            if len(activity_by_day_intervals) == 4:
                activity_by_day_intervals.pop(3)

            period_1 =  activity_by_day_intervals[0].reset_index(drop=True)
            period_2 = activity_by_day_intervals[1].reset_index(drop=True)
            sleep_period = sleep_segment.reset_index(drop=True)
            period_3 = activity_by_day_intervals[2].reset_index(drop=True)

            afternoon_avg = percentageTimeSpentForSeries(period_1)
            afternoon_avgs_percent.append(afternoon_avg)

            evening_avg = percentageTimeSpentForSeries(sleep_period)
            evening_avgs_percent.append(evening_avg)

            overnight_avg = percentageTimeSpentForSeries(period_2)
            overnight_avgs_percent.append(overnight_avg)

            morning_avg = percentageTimeSpentForSeries(period_3)
            morning_avgs_percent.append(morning_avg)

            afternoon_avg = averageBoutLengthForSeries(period_1)
            afternoon_avgs_boutlength.append(afternoon_avg)

            evening_avg = averageBoutLengthForSeries(period_2)
            evening_avgs_boutlength.append(evening_avg)

            overnight_avg = averageBoutLengthForSeries(sleep_period)
            overnight_avgs_boutlength.append(overnight_avg)

            morning_avg = averageBoutLengthForSeries(period_3)
            morning_avgs_boutlength.append(morning_avg)

            afternoon_avg = numberOfBoutsForSeries(period_1)
            afternoon_avgs_boutnumber.append(afternoon_avg)

            evening_avg = numberOfBoutsForSeries(period_2)
            evening_avgs_boutnumber.append(evening_avg)

            overnight_avg = numberOfBoutsForSeries(sleep_period)
            overnight_avgs_boutnumber.append(overnight_avg)

            morning_avg = numberOfBoutsForSeries(period_3)
            morning_avgs_boutnumber.append(morning_avg)

        activity_features_per_person.append(float(sum(afternoon_avgs_percent)/len(afternoon_avgs_percent)))
        activity_features_per_person.append(float(sum(evening_avgs_percent)/len(evening_avgs_percent)))
        activity_features_per_person.append(float(sum(overnight_avgs_percent)/len(overnight_avgs_percent)))
        activity_features_per_person.append(float(sum(morning_avgs_percent)/len(morning_avgs_percent)))

        activity_features_per_person.append(float(sum(afternoon_avgs_boutlength)/len(afternoon_avgs_boutlength)))
        activity_features_per_person.append(float(sum(evening_avgs_boutlength)/len(evening_avgs_boutlength)))
        activity_features_per_person.append(float(sum(overnight_avgs_boutlength)/len(overnight_avgs_boutlength)))
        activity_features_per_person.append(float(sum(morning_avgs_percent)/len(morning_avgs_boutlength)))

        activity_features_per_person.append(float(sum(afternoon_avgs_boutnumber)/len(afternoon_avgs_boutnumber)))
        activity_features_per_person.append(float(sum(evening_avgs_boutnumber)/len(evening_avgs_boutnumber)))
        activity_features_per_person.append(float(sum(overnight_avgs_boutnumber)/len(overnight_avgs_boutnumber)))
        activity_features_per_person.append(float(sum(morning_avgs_boutnumber)/len(morning_avgs_boutnumber)))


    activity_per_tod.loc[len(activity_per_tod)] = activity_features_per_person
    csv_filename = 'HLAF' + os.path.sep + 'HLAF_1' + os.path.sep + str(parts[7]) + "_boutMeasures.csv"
    print(csv_filename)
    activity_per_tod.to_csv(csv_filename,index=False)

else:
    print("Incomplete time series. Not processed")
