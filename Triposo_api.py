
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import requests
import datetime


# In[2]:


# Reading the json file
insp = pd.read_json('D:\\Masters\\FALL 2018\\Big Data Technologies\\Project_Flight\\Data\\TextFile1.json', orient='records')


# In[3]:



# the values that were replaces are IATA codes which were representing multiple airports in the same city
# WAS is common IATA code for Washington Dulles International Airport (IATA: IAD), Baltimore–Washington International Airport (IATA: BWI) and Ronald Reagan Washington National Airport (IATA: DCA).
# NYC collectively refers to John F. Kennedy International Airport (IATA: JFK), LaGuardia Airport (IATA: LGA), Newark Liberty International Airport (IATA: EWR), and Stewart International Airport (IATA: SWF).
# DTT is common IATA code for Detroit Metropolitan Airport (IATA: DTW), Coleman A. Young International Airport (IATA: DET) and Willow Run Airport (IATA: YIP).
# YMQ is common IATA code for Montréal–Pierre Elliott Trudeau International Airport (IATA: YUL), Montréal–Mirabel International Airport (IATA: YMX) and Montreal Saint-Hubert Longueuil Airport (IATA: YHU).
# PAR is common IATA code for Charles de Gaulle Airport (IATA: CDG), Orly Airport (IATA: ORY), Paris–Le Bourget Airport (IATA: LBG), Beauvais–Tillé Airport (IATA: BVA), Pontoise – Cormeilles Aerodrome (IATA: POX), Châlons Vatry Airport (IATA: XCR) and Vélizy – Villacoublay Air Base (IATA: VIY).
# LON is common IATA code for Heathrow Airport (IATA: LHR), Gatwick Airport (IATA: LGW), Luton Airport (IATA: LTN), London Stansted Airport (IATA: STN), London City Airport (IATA: LCY), London Southend Airport (IATA: SEN) and London Biggin Hill Airport (IATA: BQH).
# STO is common IATA code for Stockholm Arlanda Airport (IATA: ARN), Stockholm Bromma Airport (IATA: BMA), Stockholm Skavsta Airport (IATA: NYO) and Stockholm Västerås Airport (IATA: VST).
# ROM is common IATA code for Leonardo da Vinci–Fiumicino Airport (IATA: FCO) and Ciampino–G. B. Pastine International Airport (IATA: CIA).
# SEL is common IATA code for Incheon International Airport (IATA: ICN), Gimpo International Airport (IATA: GMP) and Seoul Air Base (IATA: SSN).
# TYO is common IATA code for Narita International Airport (IATA: NRT), Haneda Airport (IATA: HND) and Yokota Air Base (IATA: OKO).
insp = insp.replace(['WAS', 'NYC', 'DTT', 'YMQ', 'PAR', 'LON', 'STO', 'ROM', 'SEL', 'TYO'],
                          ['IAD', 'JFK', 'DTW', 'YUL', 'CDG', 'LHR', 'ARN', 'FCO', 'ICN', 'NRT'])


#insp.tail()


# In[4]:


# Replacing Values of city names with special characters in their name and removing duplicates

city_ap = insp.loc[:,['City','destination']]
city_ap.drop_duplicates(keep='first', inplace=True)
city_ap = city_ap.reset_index(drop=True)
city_ap = city_ap.replace(['Washington DC - All airports','Cancun','Orange County','Bogota','Krakow','San Jose'],['Washington DC','Cancún', 'Santa Ana','Bogotá','Kraków','San José'])


# In[5]:


# Creating the column for input into the location API 

city_ap.loc[:,'TriposoIn'] = 'trigram:'+city_ap.loc[:,'City']
#city_ap.head()


# In[6]:


# Creating dataframe to store results of the location API calls

loc_id = pd.DataFrame(columns=['name','parent_id','country_id','latitude','longitude','location_id','iata'])


# In[7]:


# Function to obtain all the location ids for the cities based on trigram from the location API of triposo

def locID(cities,loc_id):
    for i in range(cities.shape[0]):
        params = {
           
             'annotate':cities.loc[i,'TriposoIn'],
             'trigram' :'>=0.7',
             'account':'', # enter account details from triposo API
             'token':'', # enter token details from triposo API
            'fields':'name,country_id,coordinates,parent_id,type,id'
                }
        r = requests.get('https://www.triposo.com/api/20180627/location.json?',params=params)
        data = r.json()
        
        name = []
        parent_id = []
        country_id = []
        latitude = []
        longitude = []
        location_id = []
        iata = []
        for j in range(len(data["results"])):
            name.append(data["results"][j].get('name'))
            parent_id.append(data["results"][j].get('parent_id'))
            country_id.append(data["results"][j].get('country_id'))
            latitude.append(data["results"][j].get('coordinates')['latitude'])
            longitude.append(data["results"][j].get('coordinates')['longitude'])
            location_id.append(data["results"][j].get('id'))
            iata.append(cities.loc[i,'destination'])
        loc_id_inter = pd.DataFrame(list(zip(name,parent_id,country_id,latitude,longitude,location_id,iata)),
                                   columns=['name','parent_id','country_id','latitude','longitude','location_id','iata'])
        
        loc_id = loc_id.append(loc_id_inter)
    return loc_id
        
        


# In[8]:


# Calling function to get all possible location IDs for the city names

Location_ID = locID(city_ap,loc_id)
Location_ID = Location_ID.reset_index(drop=True)


# In[9]:


# Obtaining the airport data with coordinates to check the distance and eleminate non-relevant data

ap_coor = pd.read_csv('https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat', names = ["Aiport_ID", "Name", "City", "Country","IATA", "ICAO", "Latitude", "Longitude","Altitude","TimeZone","DST","TZ_DB","Type","Source"])
ap_coor.drop(["Aiport_ID","Altitude","TimeZone","DST","TZ_DB","Type","Source"], axis=1, inplace=True)


# In[10]:


# Checking if any cities are missed out
#ap_coor[ap_coor['IATA'].isin(Location_ID['iata'].unique())]
# Location_ID.loc[~Location_ID['iata'].isin(ap_coor['IATA']),'iata'].unique()
# len(temp.iata.unique())
# city_ap.loc[~city_ap['destination'].isin(temp['iata']),['City','destination']]


# In[11]:


#Merging on IATA for the location_id obtained from the API and the IATA from the Airport data

temp = pd.merge(Location_ID,ap_coor, left_on='iata', right_on='IATA')


#Calculating the distance between coordinate points

temp['distance']=np.sqrt(((temp.latitude-temp.Latitude)**2)+((temp.longitude-temp.Longitude)**2))

#temp

#Getting the lowest distance from airport location to get the correct location ID
# then dropping unnecessary variables

temp1 = temp.loc[temp.groupby("IATA")["distance"].idxmin()]
temp1 = temp1.reset_index(drop=True)
temp1.drop(['ICAO','Latitude','Longitude','distance','IATA'],axis=1,inplace=True)


# In[12]:


# Joining the original inspiration and the the obtained location ID on IATA codes
# Reordering the data and renaming the columns

final = pd.merge(insp,temp1,left_on='destination',right_on='iata')
final.drop(['id', 'City_x', 'iata','City_y','parent_id','country_id'], axis=1,inplace=True)

final.rename(columns={'name': 'City', 'Name': 'Airport'}, inplace=True)

final = final[['origin','destination','Airport','airline', 'price', 'departure_date','return_date', 
               'City','Country','latitude','longitude','location_id']]


# In[13]:


# Writing to disk if required.

# writer = pd.ExcelWriter('D:\\Masters\\FALL 2018\\Big Data Technologies\\Project_Flight\\Data\\loc_ID.xlsx')
# final.to_excel(writer,'Sheet1')
# writer.save()


# In[14]:


# Getting the attractions for the location IDS.

id_loc = final.location_id.unique()
attracts = pd.DataFrame(columns = ['Location_id','Category','Attraction','Snippet'])


# In[15]:


#Function to retrive the name and snippet for 4 catergories, kids,culture,romatic,adventuresports

def attraction(id_loc,attracts):
    for i in range(len(id_loc)):
        params1 = {
           
             'location_id':id_loc[i],
             'tag_labels' :'zoos|watersports|wildlife|exploringnature|character-Kid_friendly|camping|beaches|amusementparks',
             'account':'', # enter account details from triposo API
             'token':'', # enter token details from triposo API
            'fields':'name,snippet'
                }
        r1 = requests.get('https://www.triposo.com/api/20180627/poi.json?',params=params1)
        data1 = r1.json()
        
        Category1 = []
        loc_id1 = []
        Att1 = []
        Snip1 = []
        
        for j1 in range(len(data1["results"])):
            Category1.append("Kids")
            loc_id1.append(id_loc[i])
            Att1.append(data1["results"][j1].get('name'))
            Snip1.append(data1["results"][j1].get('snippet'))
            
        attracts_int1 = pd.DataFrame(list(zip(loc_id1,Category1,Att1,Snip1)),
                                     columns=['Location_id','Category','Attraction','Snippet'])
        
        
        
        params2 = {
           
             'location_id':id_loc[i],
             'tag_labels' :'architecture|hoponhopoff|museums|sightseeing|showstheatresandmusic|walkingtours',
             'account':'', # enter account details from triposo API
             'token':'', # enter token details from triposo API
            'fields':'name,snippet'
                }
        r2 = requests.get('https://www.triposo.com/api/20180627/poi.json?',params=params2)
        data2 = r2.json()
        
        Category2 = []
        loc_id2 = []
        Att2 = []
        Snip2 = []
        
        for j2 in range(len(data2["results"])):
            Category2.append("Culture")
            loc_id2.append(id_loc[i])
            Att2.append(data2["results"][j2].get('name'))
            Snip2.append(data2["results"][j2].get('snippet'))
            
        attracts_int2 = pd.DataFrame(list(zip(loc_id2,Category2,Att2,Snip2)),
                                     columns=['Location_id','Category','Attraction','Snippet'])
        
        
        params3 = {
           
             'location_id':id_loc[i],
             'tag_labels' :'wineries|character-Romantic|feature-Live_music|cuisine-Fine_dining',
             'account':'', # enter account details from triposo API
             'token':'', # enter token details from triposo API
            'fields':'name,snippet'
                }
        r3 = requests.get('https://www.triposo.com/api/20180627/poi.json?',params=params3)
        data3 = r3.json()
        
        Category3 = []
        loc_id3 = []
        Att3 = []
        Snip3 = []
        
        for j3 in range(len(data3["results"])):
            Category3.append("Romantic")
            loc_id3.append(id_loc[i])
            Att3.append(data3["results"][j3].get('name'))
            Snip3.append(data3["results"][j3].get('snippet'))
            
        attracts_int3 = pd.DataFrame(list(zip(loc_id3,Category3,Att3,Snip3)),
                                     columns=['Location_id','Category','Attraction','Snippet'])
        
        
        params4 = {
           
             'location_id':id_loc[i],
             'tag_labels' :'adrenaline|diving|fishing|hiking|hunting|kayaking|rafting|sailing|surfing|wintersport|watersports|sports',
             'account':'', # enter account details from triposo API
             'token':'', # enter token details from triposo API
            'fields':'name,snippet'
                }
        r4 = requests.get('https://www.triposo.com/api/20180627/poi.json?',params=params4)
        data4 = r4.json()
        
        Category4 = []
        loc_id4 = []
        Att4 = []
        Snip4 = []
        
        for j4 in range(len(data4["results"])):
            Category4.append("Adventure/Sports")
            loc_id4.append(id_loc[i])
            Att4.append(data4["results"][j4].get('name'))
            Snip4.append(data4["results"][j4].get('snippet'))
            
        attracts_int4 = pd.DataFrame(list(zip(loc_id4,Category4,Att4,Snip4)),
                                     columns=['Location_id','Category','Attraction','Snippet'])
        
        
        attracts = attracts.append([attracts_int1,attracts_int2,attracts_int3,attracts_int4])
    return attracts


# In[16]:


#Storing the data

findins = attraction(id_loc,attracts)
#findins

#Checking to see if any is missing
#np.setdiff1d(final.location_id.unique(),findins.Location_id.unique())

# Reindexing
findins = findins.reset_index(drop=True)
#findins


# In[17]:


#findins.head()


# In[18]:


#final[final['location_id']=='Dallas']


# In[19]:


result = pd.merge(final,findins,left_on='location_id',right_on='Location_id')
result.drop(['Location_id'],axis = 1, inplace=True)
result.head()


# In[20]:


result.dtypes


# In[21]:


# Changing to date format

result['departure_date']=pd.to_datetime(result['departure_date'], format='%Y-%m-%d')
result['return_date']=pd.to_datetime(result['return_date'], format='%Y-%m-%d')
#result.head()
                                        


# In[24]:


#Extracting the features of the date

result['departure_month'] = result['departure_date'].apply(lambda x: x.strftime("%B"))
result['departure_year'] = result['departure_date'].apply(lambda x: x.strftime("%Y"))
result['departure_day'] = result['departure_date'].apply(lambda x: x.strftime("%d"))

result.head()


# In[25]:


# Writing to disk

# writer = pd.ExcelWriter('D:\\Masters\\FALL 2018\\Big Data Technologies\\Project_Flight\\Data\\inspiration_res.xlsx')
# result.to_excel(writer,'Sheet1')
# writer.save()

