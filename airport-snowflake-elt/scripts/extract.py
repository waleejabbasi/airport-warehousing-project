import pandas as pd


def extract_data():
    csv_file = '/opt/airflow/dags/Airline Dataset Updated - v2.csv'
    df1=pd.read_csv(csv_file)

    #df_copy=df1.copy()
    #check for null values
    #print(df1.isna().sum())

    #check for duplicates
    #print(df1.duplicated())

    #check for data types
    #print(df1.dtypes)

    #correcting format for departure date
    df1["Departure Date"]=pd.to_datetime(df1["Departure Date"].astype(str).str.replace("-","/"),errors="coerce")

    #renaming column name 
    df1.rename(columns={
        'Passenger ID':'passenger_id',
        'First Name':'first_name',
        'Last Name':'last_name',
        'Airport Name':'airport_name',
        'Airport Country Code':'airport_country_code',
        'Country Name':'country_name',
        'Airport Continent':'airport_continent',
        'Departure Date':'departure_date',
        'Arrival Airport':'arrival_airport',
        'Pilot Name':'pilot_name',
        'Flight Status':'flight_status'
    },inplace=True)

    #print(df1.columns)
    return df1

if __name__=="__main__":
    extract_data()