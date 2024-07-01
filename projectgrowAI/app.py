from flask import Flask, render_template
from data_analysis import DataAnalysis

app = Flask(__name__)

# Define database connection parameters
host = 'localhost'
database = 'datas'
user = 'root'
password = 'rojo121john'

# Route to display analysis results
@app.route('/')
def display_results():
    # Create instance of DataAnalysis inside the route function
    data_analysis = DataAnalysis(host, database, user, password)

    results = {
        "Unique Trip IDs": data_analysis.unique_trip_ids(),
        "Unique Drivers": data_analysis.unique_drivers(),
        "Total Earnings": data_analysis.total_earnings(),
        "Completed Rides": data_analysis.completed_rides(),
        "Total Searches": data_analysis.total_searches(),
        "Searches with Estimate": data_analysis.searches_with_estimate(),
        "Searches for Quotes": data_analysis.searches_for_quotes(),
        "Searches got Quotes": data_analysis.searches_got_quotes(),
        "Rides Canceled by Drivers": data_analysis.rides_canceled_by_drivers(),
        "Rides with OTP Entered": data_analysis.otp_entered_rides(),
        "Total Completed Trips": data_analysis.total_completed_trips(),
        "Bookings Cancelled by Drivers": data_analysis.bookings_cancelled_by_drivers(),
        "Bookings Cancelled by Customers": data_analysis.bookings_cancelled_by_customers(),
        "Total Distance Travelled": data_analysis.total_distance_travelled(),
        "Average Distance per Trip": data_analysis.average_distance_per_trip(),
        "Average Fare per Trip": data_analysis.average_fare_per_trip(),
        "Total Searches that Got Quotes": data_analysis.total_searches_got_quotes(),
        "Top Two Locations by Trip Count": data_analysis.top_two_locations_by_trip_count(),
        "Top 5 Earning Drivers": data_analysis.top_5_earning_drivers(),
        "Duration with Highest Trip Count": data_analysis.duration_highest_trip_count(),
        "Rate of Searches to Estimates": data_analysis.searches_to_estimates_rate(),
        "Rate of Estimates to Searches for Quotes": data_analysis.estimates_to_searches_for_quotes_rate(),
        "Quote Acceptance Rate": data_analysis.quote_acceptance_rate(),
        "Area with Highest Fares": data_analysis.area_highest_fares(),
        "Area with Highest Cancellations": data_analysis.area_highest_cancellations(),
        "Area with Highest Trip Count": data_analysis.area_highest_trip_count(),
        "Duration with Highest Trip Count and Fares": data_analysis.duration_highest_trips_and_fares(),
        "Booking Cancellation Rate": data_analysis.booking_cancellation_rate(),
    }

    # Ensure quote acceptance rate is properly formatted
    if results["Quote Acceptance Rate"] is not None:
        results["Quote Acceptance Rate"] = f"{results['Quote Acceptance Rate']:.2f}%"

    return render_template('index.html', results=results)

if __name__ == '__main__':
    app.run(debug=True)
