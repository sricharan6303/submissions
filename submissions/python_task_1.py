import csv

import pandas as pd

from io import StringIO

d=pd.read_csv("C:/Users/srich/OneDrive/Desktop/dataset-1.csv")

d.rename(columns = {'ï»¿id_1':'id_1'}, inplace = True)

print(d.head())

data_list = [d.columns.tolist()] + d.astype(str).values.tolist()

data=data_list

def generate_car_matrix(df):

 

  car_df = df[['id_1', 'id_2', 'car']].pivot(index='id_1', columns='id_2', values='car').fillna(0)

  

  return car_df

def get_type_count(df)->dict:

 

  df['car_type'] = pd.cut(df['car'],

              bins=[float('-inf'), 15, 25, float('inf')],

              labels=['low', 'medium', 'high'],

              right=False)

  

  type_count = df['car_type'].value_counts().to_dict()

  

  type_count = dict(sorted(type_count.items()))

  return type_count

import pandas as pd

def get_bus_indexes(df):

  

  bus_mean = df['bus'].mean()

  # Identify indices where 'bus' values are greater than twice the mean

  bus_indexes = df[df['bus'] > 2 * bus_mean].index.tolist()

  # Sort the indices in ascending order

  bus_indexes.sort()

  return bus_indexes

def filter_routes(df):

  # Group by 'route' and calculate the average of 'truck' values

  route_avg_truck = df.groupby('route')['truck'].mean()

  # Filter routes where the average of 'truck' values is greater than 7

  selected_routes = route_avg_truck[route_avg_truck > 7].index.tolist()

  # Sort the list of selected routes

  selected_routes.sort()

  return selected_routes

def multiply_matrix(car_matrix):

  # Use the applymap function to apply the specified logic to each value in the DataFrame

  modified_matrix = car_matrix.applymap(lambda x: x * 0.75 if x > 20 else x * 1.25)

  # Round the values to 1 decimal place

  modified_matrix = modified_matrix.round(1)

  return modified_matrix

r=generate_car_matrix(d)

result = multiply_matrix(r)

# Display the result

print(result)

def check_time_completeness(df):

  # Combine 'startDay' and 'startTime' columns to create 'start_timestamp'

  df['start_timestamp'] = pd.to_datetime(df['startDay'] + ' ' + df['startTime'])

  # Combine 'endDay' and 'endTime' columns to create 'end_timestamp'

  df['end_timestamp'] = pd.to_datetime(df['endDay'] + ' ' + df['endTime'])

  # Group by (id, id_2) pairs and check completeness of time data

  completeness_check = (

    df.groupby(['id', 'id_2'])

    .apply(lambda group: (

      group['start_timestamp'].min() != pd.Timestamp('00:00:00') or

      group['end_timestamp'].max() != pd.Timestamp('23:59:59') or

      len(pd.date_range('00:00:00', '23:59:59', freq='15T').difference(group['start_timestamp'].dt.time.unique())) > 0 or

      len(pd.date_range('00:00:00', '23:59:59', freq='15T').difference(group['end_timestamp'].dt.time.unique())) > 0 or

      len(pd.date_range('2022-01-03', '2022-01-09').difference(group['start_timestamp'].dt.date.unique())) > 0 or

      len(pd.date_range('2022-01-03', '2022-01-09').difference(group['end_timestamp'].dt.date.unique())) > 0

    ))

  )

  return completeness_check

def time_check(df)->pd.Series:

  """

  Use shared dataset-2 to verify the completeness of the data by checking whether the timestamps for each unique (`id`, `id_2`) pair cover a full 24-hour and 7 days period

  Returns:

    list: return a boolean list

  """

  # Write your logic here

  df['start_timestamp'] = pd.to_datetime(df['startDay'] + ' ' + df['startTime'])

  # Combine 'endDay' and 'endTime' columns to create 'end_timestamp'

  df['end_timestamp'] = pd.to_datetime(df['endDay'] + ' ' + df['endTime'])

  # Group by (id, id_2) pairs and check completeness of time data

  completeness_check = (

    df.groupby(['id', 'id_2'])

    .apply(lambda group: (

      group['start_timestamp'].min() != pd.Timestamp('00:00:00') or

      group['end_timestamp'].max() != pd.Timestamp('23:59:59') or

      len(pd.date_range('00:00:00', '23:59:59', freq='15T').difference(group['start_timestamp'].dt.time.unique())) > 0 or

      len(pd.date_range('00:00:00', '23:59:59', freq='15T').difference(group['end_timestamp'].dt.time.unique())) > 0 or

      len(pd.date_range('2022-01-03', '2022-01-09').difference(group['start_timestamp'].dt.date.unique())) > 0 or

      len(pd.date_range('2022-01-03', '2022-01-09').difference(group['end_timestamp'].dt.date.unique())) > 0

    ))

  )

  return completeness_check

import pandas as pd

def check_time_completeness(df):

  # Combine 'startDay' and 'startTime' columns to create 'start_timestamp'

  try:

    df['start_timestamp'] = pd.to_datetime(df['startDay'] + ' ' + df['startTime'], format='%A %H:%M:%S')

  except pd.errors.OutOfBoundsDatetime as e:

    print(f"Error: {e}")

    return None

  # Combine 'endDay' and 'endTime' columns to create 'end_timestamp'

  try:

    df['end_timestamp'] = pd.to_datetime(df['endDay'] + ' ' + df['endTime'], format='%A %H:%M:%S')

  except pd.errors.OutOfBoundsDatetime as e:

    print(f"Error: {e}")

    return None

  # Group by (id, id_2) pairs and check completeness of time data

  completeness_check = (

    df.groupby(['id', 'id_2'])

    .apply(lambda group: (

      group['start_timestamp'].min() != pd.Timestamp('00:00:00') or

      group['end_timestamp'].max() != pd.Timestamp('23:59:59') or

      len(pd.date_range('00:00:00', '23:59:59', freq='15T').difference(group['start_timestamp'].dt.time.unique())) > 0 or

      len(pd.date_range('00:00:00', '23:59:59', freq='15T').difference(group['end_timestamp'].dt.time.unique())) > 0 or

      len(pd.date_range('2022-01-03', '2022-01-09').difference(group['start_timestamp'].dt.date.unique())) > 0 or

      len(pd.date_range('2022-01-03', '2022-01-09').difference(group['end_timestamp'].dt.date.unique())) > 0

    ))

  )

  return completeness_check

d2=pd.read_csv("C:/Users/srich/Downloads/dataset-2.csv")

print(check_time_completeness(d2))

