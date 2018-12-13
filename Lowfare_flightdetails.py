
# coding: utf-8

# In[1]:


import requests
import os
import datetime
import pandas as pd


# In[2]:


# creating the data frame to hold the data for all combinationtions of flights and travel times

# needs to be done only once, all the intermediate flight details are then appended to it

lf_details = pd.DataFrame(columns=['bookingTime','duration','departure','arrival','origin','destination','airline','flightNumber', 'aircraft', 'ticketClass','seatRemaining','fare', 'tax', 'refundable', 'changePenalities'])


# In[3]:


# Creating the date range

r1 = pd.date_range(datetime.datetime.strptime("2018-12-02", "%Y-%m-%d"), periods=7).strftime('%Y-%m-%d').tolist()
r2 = pd.date_range(datetime.datetime.strptime("2018-12-23", "%Y-%m-%d"), periods=7).strftime('%Y-%m-%d').tolist()

travelDates = r1+r2


# In[4]:


# Creating the destinations

destinations = ['LAX','NYC','MIA','DEN']   


# In[5]:


# Function for making the API call and returning the data frame with all the flight details

def LFflight(travelDates,destinations,lf_details):
    for i in range(len(travelDates)):
        for j in range(len(destinations)):
            params = {
                        'apikey': '', # Enter API key
                        'origin': 'ORD',
                        'destination': destinations[j],
                        'departure_date': travelDates[i],
                        'nonstop': 'true',
                        'number_of_results' : '5'
                    }
            r = requests.get('https://api.sandbox.amadeus.com/v1.2/flights/low-fare-search',params=params)

            try:
                r.raise_for_status()
            except requests.exceptions.HTTPError: ### figure out how to rerun in case of error
                pass
## READ for robust API calls: https://powerfulpython.com/blog/making-unreliable-apis-reliable-with-python/
            data = r.json()  

#Structuring the data
            bookingTime = []
            duration = []
            departure = []
            arrival = []
            origin = []
            destination = []
            airline = []
            flightNumber = []
            aircraft = []
            ticketClass = []
            seatRemaining = []
            fare = []
            tax = []
            refundable = []
            changePenalities = []

            for result in data["results"]:
                for attribute in result:
                    for deets in result[attribute]:
                        if attribute=="itineraries":
                            bookingTime.append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                            duration.append(deets.get('outbound')['duration']) 
                            departure.append(deets.get('outbound')['flights'][0].get('departs_at'))
                            arrival.append(deets.get('outbound')['flights'][0].get('arrives_at'))
                            origin.append(deets.get('outbound')['flights'][0].get('origin').get('airport'))
                            destination.append(deets.get('outbound')['flights'][0].get('destination').get('airport'))
                            airline.append(deets.get('outbound')['flights'][0].get('operating_airline'))
                            flightNumber.append(deets.get('outbound')['flights'][0].get('flight_number'))
                            aircraft.append(deets.get('outbound')['flights'][0].get('aircraft'))
                            ticketClass.append(deets.get('outbound')['flights'][0].get('booking_info').get('travel_class'))
                            seatRemaining.append(deets.get('outbound')['flights'][0].get('booking_info').get('seats_remaining'))
                            fare.append(result['fare'].get('price_per_adult').get('total_fare'))
                            tax.append(result['fare'].get('price_per_adult').get('tax'))
                            refundable.append(result['fare'].get('restrictions').get('refundable'))
                            changePenalities.append(result['fare'].get('restrictions').get('change_penalties'))

#Creating a dataframe of the details collected
            lf_details_intermediate = pd.DataFrame(list(zip(bookingTime, duration, departure,arrival,origin,destination,airline,flightNumber,aircraft,ticketClass,seatRemaining,fare,tax,refundable,changePenalities))
                                                  ,columns=['bookingTime','duration','departure','arrival','origin','destination','airline','flightNumber', 'aircraft', 'ticketClass','seatRemaining','fare', 'tax', 'refundable', 'changePenalities'])
# Appending all the intermediate results together            
            lf_details = lf_details.append(lf_details_intermediate)
    return lf_details


# In[6]:


result = LFflight(travelDates,destinations,lf_details)


# In[7]:


# Some observations did not take the destination as per the list but it was in the vicinity like (LGA,EWR) for NYC, etc.
# need to still extract the time component from departure/arrival
# need to reindex the dataframe.
result

