import pandas as pd
from sqlalchemy import create_engine
from mysql.connector import Error

class DataAnalysis:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password

        self.registration_trends = None
        self.all_cities_table = None
        self.trip_trends = None

        try:
            self.engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")
            self.registration_trends = pd.read_sql("SELECT * FROM `all-time registration trends`", self.engine)
            self.all_cities_table = pd.read_sql("SELECT * FROM `all-time_table_all_cities_modified`", self.engine)
            self.trip_trends = pd.read_sql("SELECT * FROM `trip_trends_corrected`", self.engine)
            self.convert_columns_to_numeric()
            print("Connected to MySQL database")
        except Error as e:
            print(f"Error: {e}")
            self.engine = None

    def clean_column(self, column):
        return column.str.replace('[^0-9.]', '', regex=True).astype(float)

    def convert_columns_to_numeric(self):
        if self.all_cities_table is not None:
            self.all_cities_table['Searches'] = self.clean_column(self.all_cities_table['Searches'])
            self.all_cities_table['Searches_which_got_estimate'] = self.clean_column(self.all_cities_table['Searches_which_got_estimate'])
            self.all_cities_table['Searches_for_Quotes'] = self.clean_column(self.all_cities_table['Searches_for_Quotes'])
            self.all_cities_table['Searches_which_got_Quotes'] = self.clean_column(self.all_cities_table['Searches_which_got_Quotes'])
            self.all_cities_table["Drivers_Earnings"] = self.clean_column(self.all_cities_table["Drivers_Earnings"])
            self.all_cities_table['Average_Fare_per_Trip'] = self.clean_column(self.all_cities_table['Average_Fare_per_Trip'])
            self.all_cities_table['Bookings'] = self.clean_column(self.all_cities_table['Bookings'])
            self.all_cities_table['Cancelled_Bookings'] = self.clean_column(self.all_cities_table['Cancelled_Bookings'])
            self.all_cities_table['Completed_Trips'] = self.clean_column(self.all_cities_table['Completed_Trips'])
            self.all_cities_table['Booking_Cancellation_Rate'] = self.clean_column(self.all_cities_table['Booking_Cancellation_Rate'].str.rstrip('%'))

    def unique_trip_ids(self):
        if self.trip_trends is not None:
            return self.trip_trends['Completed Trips'].nunique()
        return None

    def unique_drivers(self):
        if self.trip_trends is not None:
            return self.trip_trends["Drivers Earnings"].nunique()
        return None

    def total_earnings(self):
        if self.trip_trends is not None:
            return self.trip_trends["Drivers Earnings"].sum()
        return None

    def completed_rides(self):
        if self.trip_trends is not None:
            return self.trip_trends['Completed Trips'].sum()
        return None

    def total_searches(self):
        if self.trip_trends is not None:
            return self.trip_trends['Searches'].sum()
        return None

    def searches_with_estimate(self):
        if self.trip_trends is not None and 'Searches_which_got_estimate' in self.trip_trends.columns:
            return self.trip_trends['Searches_which_got_estimate'].sum()
        return None

    def searches_for_quotes(self):
        if self.trip_trends is not None and 'Searches_for_Quotes' in self.trip_trends.columns:
            return self.trip_trends['Searches_for_Quotes'].sum()
        return None

    def searches_got_quotes(self):
        if self.trip_trends is not None and 'Searches_which_got_Quotes' in self.trip_trends.columns:
            return self.trip_trends['Searches_which_got_Quotes'].sum()
        return None

    def rides_canceled_by_drivers(self):
        if self.trip_trends is not None and 'Driver Cancellation Rate' in self.trip_trends.columns:
            total_rides = self.trip_trends['Completed Trips'].sum()
            driver_cancellations = self.trip_trends['Driver Cancellation Rate'].mean() * total_rides
            return driver_cancellations
        return None

    def otp_entered_rides(self):
        return None

    def total_completed_trips(self):
        if self.all_cities_table is not None:
            return self.all_cities_table['Completed_Trips'].sum()
        return None

    def bookings_cancelled_by_drivers(self):
        if self.all_cities_table is not None:
            return self.all_cities_table['Bookings'].sum() - self.all_cities_table['Cancelled_Bookings'].sum()
        return None

    def bookings_cancelled_by_customers(self):
        if self.all_cities_table is not None:
            return self.all_cities_table['Bookings'].sum() * (self.all_cities_table['User_Cancellation_Rate'].str.rstrip('%').astype(float) / 100).mean()
        return None

    def total_distance_travelled(self):
        if self.all_cities_table is not None:
            return self.all_cities_table['Distance_Travelled_km'].sum()
        return None

    def average_distance_per_trip(self):
        if self.all_cities_table is not None:
            return self.all_cities_table['Average_Distance_per_Trip_km'].mean()
        return None

    def average_fare_per_trip(self):
        if self.all_cities_table is not None:
            return self.all_cities_table['Average_Fare_per_Trip'].mean()
        return None

    def total_searches_got_quotes(self):
        if self.all_cities_table is not None:
            return self.all_cities_table['Searches_which_got_Quotes'].sum()
        return None

    def top_two_locations_by_trip_count(self):
        if self.all_cities_table is not None:
            return self.all_cities_table[['City', 'Completed_Trips']].sort_values(by='Completed_Trips', ascending=False).head(2)
        return None

    def top_5_earning_drivers(self):
        if self.all_cities_table is not None:
            return self.all_cities_table.nlargest(5, "Drivers_Earnings")[['City', "Drivers_Earnings"]]
        return None

    def duration_highest_trip_count(self):
        if self.trip_trends is not None:
            self.trip_trends['Completed Trips'] = self.trip_trends['Completed Trips'].replace('[,]', '', regex=True).astype(int)
            return self.trip_trends.nlargest(1, 'Completed Trips')[['Time', 'Completed Trips']]
        return None

    def searches_to_estimates_rate(self):
        if self.all_cities_table is not None:
            total_searches = self.all_cities_table['Searches'].sum()
            searches_with_estimate = self.all_cities_table['Searches_which_got_estimate'].sum()
            if total_searches > 0:
                return (searches_with_estimate / total_searches) * 100
        return None

    def estimates_to_searches_for_quotes_rate(self):
        if self.all_cities_table is not None:
            searches_with_estimate = self.all_cities_table['Searches_which_got_estimate'].sum()
            searches_for_quotes = self.all_cities_table['Searches_for_Quotes'].sum()
            if searches_for_quotes > 0:
                return (searches_with_estimate / searches_for_quotes) * 100
        return None

    def area_highest_cancellations(self):
        if self.all_cities_table is not None:
            return self.all_cities_table[['City', 'Booking_Cancellation_Rate']].nlargest(1, 'Booking_Cancellation_Rate')
        return None

    def area_highest_trip_count(self):
        if self.all_cities_table is not None:
            return self.all_cities_table[['City', 'Completed_Trips']].nlargest(1, 'Completed_Trips')
        return None

    def area_highest_fares(self):
        if self.all_cities_table is not None:
            return self.all_cities_table[['City', 'Drivers_Earnings']].nlargest(1, 'Drivers_Earnings')
        return None

    def duration_highest_trips_and_fares(self):
        if self.trip_trends is not None:
            self.trip_trends['Completed Trips'] = self.trip_trends['Completed Trips'].replace('[,]', '', regex=True).astype(int)
            self.trip_trends['Drivers Earnings'] = self.trip_trends['Drivers Earnings'].replace('[^0-9.]', '', regex=True).astype(float)
            highest_trips = self.trip_trends.nlargest(1, 'Completed Trips')[['Time', 'Completed Trips']]
            highest_fares = self.trip_trends.nlargest(1, 'Drivers Earnings')[['Time', 'Drivers Earnings']]
            return pd.concat([highest_trips, highest_fares])
        return None

    def booking_cancellation_rate(self):
        if self.all_cities_table is not None:
            total_bookings = self.all_cities_table['Bookings'].sum()
            cancelled_bookings = self.all_cities_table['Cancelled_Bookings'].sum()
            if total_bookings > 0:
                return (cancelled_bookings / total_bookings) * 100
        return None

    def quote_acceptance_rate(self):
        if self.all_cities_table is not None:
            searches_with_estimate = self.all_cities_table['Searches_which_got_estimate'].sum()
            searches_which_got_quotes = self.all_cities_table['Searches_which_got_Quotes'].sum()
            if searches_which_got_quotes > 0:
                return (searches_with_estimate / searches_which_got_quotes) * 100
        return None

    # Add other methods as needed

    def clean_column(self, column):
        return column.str.replace('[^0-9.]', '', regex=True).astype(float)

    def convert_columns_to_numeric(self):
        if self.all_cities_table is not None:
            self.all_cities_table['Searches'] = self.clean_column(self.all_cities_table['Searches'])
            self.all_cities_table['Searches_which_got_estimate'] = self.clean_column(self.all_cities_table['Searches_which_got_estimate'])
            self.all_cities_table['Searches_for_Quotes'] = self.clean_column(self.all_cities_table['Searches_for_Quotes'])
            self.all_cities_table['Searches_which_got_Quotes'] = self.clean_column(self.all_cities_table['Searches_which_got_Quotes'])
            self.all_cities_table["Drivers_Earnings"] = self.clean_column(self.all_cities_table["Drivers_Earnings"])
            self.all_cities_table['Average_Fare_per_Trip'] = self.clean_column(self.all_cities_table['Average_Fare_per_Trip'])
            self.all_cities_table['Bookings'] = self.clean_column(self.all_cities_table['Bookings'])
            self.all_cities_table['Cancelled_Bookings'] = self.clean_column(self.all_cities_table['Cancelled_Bookings'])
            self.all_cities_table['Completed_Trips'] = self.clean_column(self.all_cities_table['Completed_Trips'])
            self.all_cities_table['Booking_Cancellation_Rate'] = self.clean_column(self.all_cities_table['Booking_Cancellation_Rate'].str.rstrip('%'))
