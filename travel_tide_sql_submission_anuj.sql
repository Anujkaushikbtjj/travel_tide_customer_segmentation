/* 
The main goal of the project is to segment customers of Travel Tide and assign them pre-defined perks.
Each customer must be assigned one and only one perk.
Following perks are avialbale:
• free hotel meal
• free checked bag
• no cancellation fees
• exclusive discounts
• 1 night free hotel with flight

The following query was written as part of the mastery project for completing
7 months intense Data Analytics program at Masterschool. The query was written using Beekeper studio in postgresql.
*/

/*
The following query returns the session after '2023-01-04'
*/
--creating a CTE for sessions after '2023-01-04'

WITH sessions_2023 AS(
SELECT
	*
FROM
	sessions
WHERE 
	session_start > '2023-01-04'
),

--create CTEs to filter users with more than 7 sessions and call repeat_users

repeat_users AS(
SELECT 
	user_id, 
  COUNT(*)
FROM 
	sessions_2023
GROUP BY 
	user_id
HAVING 
	COUNT(*) >7
),

/* 
The following query combines repeat_users with sessions after '2023-01-04' and join with other tables
i.e. flights, hotels and users to get all information
*/
--combine above two CTEs tables with filtered users and sessions from 2023

filterd_users_sessions AS(
SELECT --selecting all the columns needed for analysis
	sessions_2023.session_id,sessions_2023.user_id, sessions_2023.session_start, sessions_2023.session_end, sessions_2023.flight_discount,
  sessions_2023.hotel_discount, sessions_2023.flight_discount_amount, sessions_2023.hotel_discount_amount, sessions_2023.flight_booked,
  sessions_2023.hotel_booked, sessions_2023.page_clicks, sessions_2023.cancellation,
  sessions_2023.trip_id, flights.origin_airport, flights.destination, flights.destination_airport, flights.seats, flights.return_flight_booked,
  flights.departure_time, flights.return_time, flights.checked_bags, flights.trip_airline, flights.destination_airport_lat,
  flights.destination_airport_lon, flights.base_fare_usd, hotels.hotel_name, 
  CASE 
  	WHEN hotels.nights = 0 THEN 1 --convert rows with 0 nights to 1 later we will select only rows with check_out_time later than check_in time.
  	ELSE nights
  END AS nights, 
  hotels.rooms, hotels.check_in_time, hotels.check_out_time,
  hotels.hotel_per_room_usd, users.birthdate, users.gender, users.married, users.has_children, users.home_country, users.home_city,
  users.home_airport, users.home_airport_lat, users.home_airport_lon, users.sign_up_date
  
FROM
	sessions_2023
	LEFT JOIN flights ON sessions_2023.trip_id = flights.trip_id --use LEFT JOIN to retain all sessions from sessions_2023 CTE
	LEFT JOIN hotels ON sessions_2023.trip_id = hotels.trip_id
	LEFT JOIN users ON sessions_2023.user_id = users.user_id
WHERE 
	sessions_2023.user_id IN (SELECT user_id FROM repeat_users) --filter only repeat_users with >7 sessions
),

/* 
The following query clean some problematic rows 
*/
--such as check_out_time is same or earlier than check_in_time
--remove rows with return time earlier or same as departure time
--remove rows with cancelled trips
--remove rows with nights in negative numbers

cleaned_data AS(
SELECT *
FROM 
  filterd_users_sessions
WHERE 
	(return_time > departure_time OR return_time IS NULL OR departure_time IS NULL) --remove rows where return time is same as departure time or earlier
	AND (check_out_time > check_in_time OR check_out_time IS NULL OR check_in_time IS NULL) --remove rows where check_out_time is earlier than check_in_time
  AND (nights>=0 OR nights IS NULL) --remove rows where nights are in negative numbers
  AND cancellation = 'false' --remove cancelled trips
),

/* 
The following query maps the airports with their country. 
This is useful in identifying the international and domestic flights. 
*/
--map origin and destination airports to country

airports AS(
  SELECT 
    session_id, 
    trip_id, 
    user_id, 
    origin_airport, 
    destination_airport,
	CASE 
        WHEN origin_airport IN ('ACC') THEN 'Ghana'
        WHEN origin_airport IN ('ADJ', 'AMM') THEN 'Jordan'
        WHEN origin_airport IN ('AEP') THEN 'Argentina'
        WHEN origin_airport IN ('AGR', 'DEL', 'PNQ', 'JAI', 'BLR') THEN 'India'
        WHEN origin_airport IN ('AKL', 'HLZ') THEN 'New Zealand'
        WHEN origin_airport IN ('AKR') THEN 'Nigeria'
        WHEN origin_airport IN ('AMA', 'ANC', 'ATL', 'AUS', 'BFL', 'BHM', 'BNA', 'BOS', 
                                'BRO', 'BTV', 'BUF', 'BWI', 'CLE', 'CLT', 'CMH', 'COS', 
                                'DAL', 'DCA', 'DEN', 'DET', 'DSM', 'DTW', 'EWR', 'FAT', 
                                'FLO', 'FTW', 'GRR', 'HNL', 'HOU', 'IAD', 'IAH', 'ICT', 
                                'IND', 'JAX', 'JFK', 'LAS', 'LAX', 'LGA', 'LGB', 'LIT', 
                                'MCI', 'MCO', 'MDW', 'MEM', 'MIA', 'MKE', 'MSN', 'MSP', 
                                'MSY', 'OAK', 'OKC', 'OMA', 'ORD', 'ORF', 'PHL', 'PHX', 
                                'PWM', 'RIC', 'RNO', 'ROC', 'SAN', 'SAT', 'SFO', 'SJC', 
                                'SLC', 'SMF', 'SNA', 'STL', 'TPA', 'TUL', 'TUS', 
                                'YZD', 'LSV', 'NIP', 'LUK', 'YED', 'SKA', 'BIF', 'NBG', 
                                'SEA', 'PNE', 'ORL', 'DMA', 'NGU', 'LUF', 'TLH', 'EFD', 
                                'NZY', 'LCK', 'EDF', 'LBB', 'MCF', 'SFF', 'IAB', 'TCM', 
                                'BFI', 'INT', 'JNB', 'MXF', 'LNK', 'MCC', 'RIV', 'LCY', 
                                'POB', 'OPF', 'BAD', 'YND', 'LRD', 'RND', 'PIE', 'YKZ', 
                                'YHM', 'RME', 'YAV', 'PDX', 'XSP', 'SKF', 'PCB', 'MOD', 
                                'CVG', 'RAL', 'CRP', 'RKE', 'PVD', 'XFW', 'YIP', 'THF', 
                                'TIK', 'TNT', 'MOB', 'CMB', 'GEG', 'MRI', 'YXU', 'LOU', 
                                'LRF', 'SAC', 'FYV', 'ELP', 'BTR', 'SPG', 'YQG', 'YMX', 
                                'TYS', 'PHF', 'CMN', 'DLC', 'TOJ', 'OFF', 'CIA', 'YXD', 
                                'NZC', 'MHR', 'SHV', 'SCK', 'BFM', 'UGN') THEN 'USA'
        WHEN origin_airport IN ('AMS') THEN 'Netherlands'
        WHEN origin_airport IN ('ARN', 'BMA', 'NYO') THEN 'Sweden'
        WHEN origin_airport IN ('AUH', 'AZI', 'DXB') THEN 'United Arab Emirates'
        WHEN origin_airport IN ('AYT', 'IST') THEN 'Turkey'
        WHEN origin_airport IN ('BCN', 'MAD') THEN 'Spain'
        WHEN origin_airport IN ('BEY') THEN 'Lebanon'
        WHEN origin_airport IN ('BKK', 'HKT') THEN 'Thailand'
        WHEN origin_airport IN ('BOG') THEN 'Colombia'
        WHEN origin_airport IN ('BRU') THEN 'Belgium'
        WHEN origin_airport IN ('BUD') THEN 'Hungary'
        WHEN origin_airport IN ('CAI', 'HRG') THEN 'Egypt'
        WHEN origin_airport IN ('CAN', 'PEK', 'SHA', 'HKG', 'SZX', 'XMN', 'XIY', 'CTU', 'TAO', 'KWL', 'DLC') THEN 'China'
        WHEN origin_airport IN ('CDG', 'ORY', 'LBG', 'NCE') THEN 'France'
        WHEN origin_airport IN ('CPH') THEN 'Denmark'
        WHEN origin_airport IN ('CPT', 'JNB', 'DUR', 'HLA', 'VIR') THEN 'South Africa'
        WHEN origin_airport IN ('DUB') THEN 'Ireland'
        WHEN origin_airport IN ('EDI', 'LHR', 'LGW', 'LTN', 'STN', 'MAN', 'LCY') THEN 'United Kingdom'
        WHEN origin_airport IN ('FCO', 'MXP', 'LIN', 'NAP') THEN 'Italy'
        WHEN origin_airport IN ('GIG') THEN 'Brazil'
        WHEN origin_airport IN ('GVA') THEN 'Switzerland'
        WHEN origin_airport IN ('HAM', 'MUC', 'FRA', 'TXL', 'SXF', 'THF', 'XFW') THEN 'Germany'
        WHEN origin_airport IN ('HND', 'NRT', 'KIX', 'ITM', 'FUK') THEN 'Japan'
        WHEN origin_airport IN ('ICN', 'GMP') THEN 'South Korea'
        WHEN origin_airport IN ('JRS', 'TLV') THEN 'Israel'
        WHEN origin_airport IN ('KUL', 'JHB') THEN 'Malaysia'
        WHEN origin_airport IN ('LIM') THEN 'Peru'
        WHEN origin_airport IN ('LIS', 'OPO') THEN 'Portugal'
        WHEN origin_airport IN ('LOS') THEN 'Nigeria'
        WHEN origin_airport IN ('MEL', 'SYD', 'BNE', 'PER', 'BWU') THEN 'Australia'
        WHEN origin_airport IN ('MEX') THEN 'Mexico'
        WHEN origin_airport IN ('MNL', 'SJI') THEN 'Philippines'
        WHEN origin_airport IN ('OSL') THEN 'Norway'
        WHEN origin_airport IN ('OTP') THEN 'Romania'
        WHEN origin_airport IN ('PRG') THEN 'Czech Republic'
        WHEN origin_airport IN ('PUJ') THEN 'Dominican Republic'
        WHEN origin_airport IN ('RUH') THEN 'Saudi Arabia'
        WHEN origin_airport IN ('SGN', 'HAN') THEN 'Vietnam'
        WHEN origin_airport IN ('SIN', 'XSP') THEN 'Singapore'
        WHEN origin_airport IN ('SOF') THEN 'Bulgaria'
        WHEN origin_airport IN ('SVO', 'VKO') THEN 'Russia'
        WHEN origin_airport IN ('TPE', 'TSA') THEN 'Taiwan'
        WHEN origin_airport IN ('VIE') THEN 'Austria'
        WHEN origin_airport IN ('VCE') THEN 'Italy'
        WHEN origin_airport IN ('WAW') THEN 'Poland'
        WHEN origin_airport IN ('YAW', 'YEG', 'YHZ', 'YOW', 'YQB', 'YUL', 'YVR', 'YWG', 'YYC', 'YYJ', 'YYZ', 'YXE', 'YHU', 'YND', 'YTZ', 'YIP', 'YXU', 'YXD', 'YQG', 'YMX') THEN 'Canada'
				WHEN origin_airport IN ('HLP', 'DPS') THEN 'Indonesia'
				WHEN origin_airport IN ('SVZ') THEN 'Venezuela'
				WHEN origin_airport IN ('HER') THEN 'Greece'
				WHEN origin_airport IN ('NCA') THEN 'New Caledonia'
				WHEN origin_airport IN ('LSQ') THEN 'Chile'
				WHEN origin_airport IN ('SJO') THEN 'Costa Rica'
				WHEN origin_airport IN ('MFM') THEN 'Macau'
        ELSE 'Unknown'
    END AS origin_country,
    CASE 
        WHEN destination_airport IN ('ACC') THEN 'Ghana'
        WHEN destination_airport IN ('ADJ', 'AMM') THEN 'Jordan'
        WHEN destination_airport IN ('AEP') THEN 'Argentina'
        WHEN destination_airport IN ('AGR', 'DEL', 'PNQ', 'JAI', 'BLR') THEN 'India'
        WHEN destination_airport IN ('AKL', 'HLZ') THEN 'New Zealand'
        WHEN destination_airport IN ('AKR') THEN 'Nigeria'
        WHEN destination_airport IN ('AMA', 'ANC', 'ATL', 'AUS', 'BFL', 'BHM', 'BNA', 'BOS', 
                                'BRO', 'BTV', 'BUF', 'BWI', 'CLE', 'CLT', 'CMH', 'COS', 
                                'DAL', 'DCA', 'DEN', 'DET', 'DSM', 'DTW', 'EWR', 'FAT', 
                                'FLO', 'FTW', 'GRR', 'HNL', 'HOU', 'IAD', 'IAH', 'ICT', 
                                'IND', 'JAX', 'JFK', 'LAS', 'LAX', 'LGA', 'LGB', 'LIT', 
                                'MCI', 'MCO', 'MDW', 'MEM', 'MIA', 'MKE', 'MSN', 'MSP', 
                                'MSY', 'OAK', 'OKC', 'OMA', 'ORD', 'ORF', 'PHL', 'PHX', 
                                'PWM', 'RIC', 'RNO', 'ROC', 'SAN', 'SAT', 'SFO', 'SJC', 
                                'SLC', 'SMF', 'SNA', 'STL', 'TPA', 'TUL', 'TUS', 
                                'YZD', 'LSV', 'NIP', 'LUK', 'YED', 'SKA', 'BIF', 'NBG', 
                                'SEA', 'PNE', 'ORL', 'DMA', 'NGU', 'LUF', 'TLH', 'EFD', 
                                'NZY', 'LCK', 'EDF', 'LBB', 'MCF', 'SFF', 'IAB', 'TCM', 
                                'BFI', 'INT', 'JNB', 'MXF', 'LNK', 'MCC', 'RIV', 'LCY', 
                                'POB', 'OPF', 'BAD', 'YND', 'LRD', 'RND', 'PIE', 'YKZ', 
                                'YHM', 'RME', 'YAV', 'PDX', 'XSP', 'SKF', 'PCB', 'MOD', 
                                'CVG', 'RAL', 'CRP', 'RKE', 'PVD', 'XFW', 'YIP', 'THF', 
                                'TIK', 'TNT', 'MOB', 'CMB', 'GEG', 'MRI', 'YXU', 'LOU', 
                                'LRF', 'SAC', 'FYV', 'ELP', 'BTR', 'SPG', 'YQG', 'YMX', 
                                'TYS', 'PHF', 'CMN', 'DLC', 'TOJ', 'OFF', 'CIA', 'YXD', 
                                'NZC', 'MHR', 'SHV', 'SCK', 'BFM', 'UGN') THEN 'USA'
        WHEN destination_airport IN ('AMS') THEN 'Netherlands'
        WHEN destination_airport IN ('ARN', 'BMA', 'NYO') THEN 'Sweden'
        WHEN destination_airport IN ('AUH', 'AZI', 'DXB') THEN 'United Arab Emirates'
        WHEN destination_airport IN ('AYT', 'IST') THEN 'Turkey'
        WHEN destination_airport IN ('BCN', 'MAD') THEN 'Spain'
        WHEN destination_airport IN ('BEY') THEN 'Lebanon'
        WHEN destination_airport IN ('BKK', 'HKT') THEN 'Thailand'
        WHEN destination_airport IN ('BOG') THEN 'Colombia'
        WHEN destination_airport IN ('BRU') THEN 'Belgium'
        WHEN destination_airport IN ('BUD') THEN 'Hungary'
        WHEN destination_airport IN ('CAI', 'HRG') THEN 'Egypt'
        WHEN destination_airport IN ('CAN', 'PEK', 'SHA', 'HKG', 'SZX', 'XMN', 'XIY', 'CTU', 'TAO', 'KWL', 'DLC') THEN 'China'
        WHEN destination_airport IN ('CDG', 'ORY', 'LBG', 'NCE') THEN 'France'
        WHEN destination_airport IN ('CPH') THEN 'Denmark'
        WHEN destination_airport IN ('CPT', 'JNB', 'DUR', 'HLA', 'VIR') THEN 'South Africa'
        WHEN destination_airport IN ('DUB') THEN 'Ireland'
        WHEN destination_airport IN ('EDI', 'LHR', 'LGW', 'LTN', 'STN', 'MAN', 'LCY') THEN 'United Kingdom'
        WHEN destination_airport IN ('FCO', 'MXP', 'LIN', 'NAP') THEN 'Italy'
        WHEN destination_airport IN ('GIG') THEN 'Brazil'
        WHEN destination_airport IN ('GVA') THEN 'Switzerland'
        WHEN destination_airport IN ('HAM', 'MUC', 'FRA', 'TXL', 'SXF', 'THF', 'XFW') THEN 'Germany'
        WHEN destination_airport IN ('HND', 'NRT', 'KIX', 'ITM', 'FUK') THEN 'Japan'
        WHEN destination_airport IN ('ICN', 'GMP') THEN 'South Korea'
        WHEN destination_airport IN ('JRS', 'TLV') THEN 'Israel'
        WHEN destination_airport IN ('KUL', 'JHB') THEN 'Malaysia'
        WHEN destination_airport IN ('LIM') THEN 'Peru'
        WHEN destination_airport IN ('LIS', 'OPO') THEN 'Portugal'
        WHEN destination_airport IN ('LOS') THEN 'Nigeria'
        WHEN destination_airport IN ('MEL', 'SYD', 'BNE', 'PER', 'BWU') THEN 'Australia'
        WHEN destination_airport IN ('MEX') THEN 'Mexico'
        WHEN destination_airport IN ('MNL', 'SJI') THEN 'Philippines'
        WHEN destination_airport IN ('OSL') THEN 'Norway'
        WHEN destination_airport IN ('OTP') THEN 'Romania'
        WHEN destination_airport IN ('PRG') THEN 'Czech Republic'
        WHEN destination_airport IN ('PUJ') THEN 'Dominican Republic'
        WHEN destination_airport IN ('RUH') THEN 'Saudi Arabia'
        WHEN destination_airport IN ('SGN', 'HAN') THEN 'Vietnam'
        WHEN destination_airport IN ('SIN', 'XSP') THEN 'Singapore'
        WHEN destination_airport IN ('SOF') THEN 'Bulgaria'
        WHEN destination_airport IN ('SVO', 'VKO') THEN 'Russia'
        WHEN destination_airport IN ('TPE', 'TSA') THEN 'Taiwan'
        WHEN destination_airport IN ('VIE') THEN 'Austria'
        WHEN destination_airport IN ('VCE') THEN 'Italy'
        WHEN destination_airport IN ('WAW') THEN 'Poland'
        WHEN destination_airport IN ('YAW', 'YEG', 'YHZ', 'YOW', 'YQB', 'YUL', 'YVR', 'YWG', 'YYC', 'YYJ', 'YYZ', 'YXE', 'YHU', 'YND', 'YTZ', 'YIP', 'YXU', 'YXD', 'YQG', 'YMX') THEN 'Canada'
				WHEN destination_airport IN ('HLP', 'DPS') THEN 'Indonesia'
				WHEN destination_airport IN ('SVZ') THEN 'Venezuela'
				WHEN destination_airport IN ('HER') THEN 'Greece'
				WHEN destination_airport IN ('NCA') THEN 'New Caledonia'
				WHEN destination_airport IN ('LSQ') THEN 'Chile'
				WHEN destination_airport IN ('SJO') THEN 'Costa Rica'
				WHEN destination_airport IN ('MFM') THEN 'Macau'
        ELSE 'Unknown'
    END AS destination_country
FROM cleaned_data
),

--identify the international and domestic flights based on the origin and destination country
international_domestic AS(
SELECT 
	session_id,
  user_id,
  CASE 
  	WHEN origin_country = destination_country 
  		AND origin_country != 'Unknown'
  		AND origin_country IS NOT NULL
  		AND destination_country != 'Unknown'
  		AND destination_country IS NOT NULL 
  	THEN 'Domestic'
  	
  	WHEN origin_country != destination_country 
  		AND origin_country != 'Unknown'
  		AND origin_country IS NOT NULL 
  		AND destination_country != 'Unknown'
  		AND destination_country IS NOT NULL 
  	THEN 'International'
  	--ELSE 'Unknown'
  END AS international_domestic
FROM
	airports
  ),

--count number of international and domestic flights by users
user_international_domestic AS(
SELECT 
  user_id,
  COUNT(CASE WHEN international_domestic ='Domestic' THEN 1 END) AS num_domestic_flights,
  COUNT(CASE WHEN international_domestic ='International' THEN 1 END) AS num_international_flights
FROM
  international_domestic	
GROUP BY
  user_id
  ),

/* 
The following queries helps to identify user behaviour information. 
This will help in segmenting users.
*/

--create table for users session information
users_session_summary AS(
SELECT 
	user_id,
  COUNT(DISTINCT session_id) AS num_sessions,
  AVG(EXTRACT(EPOCH FROM session_end - session_start)) AS avg_session_duration_sec,
  AVG(page_clicks) AS avg_click_per_session
FROM
	cleaned_data
GROUP BY 
	user_id
), 

--creating users tavel summary
user_travel_summary AS(
SELECT
  user_id,
  COUNT(DISTINCT trip_id) AS num_trips, --count the number of trips booked
  COALESCE(SUM(CASE 
      	WHEN flight_booked= TRUE AND return_flight_booked = TRUE THEN 2
      	WHEN flight_booked= TRUE AND return_flight_booked = FALSE THEN 1
      	--ELSE 0
      END ),0) AS num_flights, --count the number of flights booked considering the return flight booked or not
  COALESCE(SUM(CASE
      	WHEN return_flight_booked IS TRUE THEN seats*2
      	ELSE seats
     		END), 0) AS total_seats, --count the total seats booked
  COALESCE(AVG(CASE
      	WHEN return_flight_booked IS TRUE THEN seats*2
      	ELSE seats
     		END),0) AS avg_seats, --calculate the average seats booked 
	COALESCE(SUM(CASE 
      	WHEN flight_discount IS FALSE AND return_flight_booked IS FALSE THEN (base_fare_usd)*seats
      	WHEN flight_discount IS FALSE AND return_flight_booked IS TRUE THEN (base_fare_usd *2)*seats
      	WHEN flight_discount IS TRUE AND return_flight_booked IS FALSE THEN (base_fare_usd - (base_fare_usd*flight_discount_amount)/100)*seats
      	WHEN flight_discount IS TRUE AND return_flight_booked IS TRUE THEN ((base_fare_usd - (base_fare_usd*flight_discount_amount)/100)*2)*seats
      END ),0) AS total_flight_spend, --total spend on flights
  COALESCE(AVG(CASE 
      	WHEN flight_discount IS FALSE AND return_flight_booked IS FALSE THEN (base_fare_usd)*seats
      	WHEN flight_discount IS FALSE AND return_flight_booked IS TRUE THEN (base_fare_usd *2)*seats
      	WHEN flight_discount IS TRUE AND return_flight_booked IS FALSE THEN (base_fare_usd - (base_fare_usd*flight_discount_amount)/100)*seats
      	WHEN flight_discount IS TRUE AND return_flight_booked IS TRUE THEN ((base_fare_usd - (base_fare_usd*flight_discount_amount)/100)*2)*seats
      END ),0) AS avg_flight_spend,--average spend on flights
  COALESCE(SUM(CASE 
      	WHEN hotel_discount IS FALSE THEN (hotel_per_room_usd)*nights*rooms
      	WHEN hotel_discount IS TRUE THEN (hotel_per_room_usd - (hotel_per_room_usd*hotel_discount_amount)/100)*nights*rooms
      END ),0) AS total_hotel_spend,--total spend on hotels
  COALESCE(AVG(CASE 
      	WHEN hotel_discount IS FALSE THEN (hotel_per_room_usd)*nights*rooms
      	WHEN hotel_discount IS TRUE THEN (hotel_per_room_usd - (hotel_per_room_usd*hotel_discount_amount)/100)*nights*rooms
      END ),0) AS avg_hotel_spend,--average spend on hotels
  COALESCE(AVG(checked_bags),0) AS avg_checked_bags, --average checked bags 
  COALESCE(AVG(EXTRACT(DAY FROM departure_time-session_end)),0) AS avg_booking_days_in_advance,
	COALESCE(SUM(haversine_distance(home_airport_lat, home_airport_lon, destination_airport_lat, destination_airport_lon)),0) AS total_km_flown,
  COALESCE(AVG(haversine_distance(home_airport_lat, home_airport_lon, destination_airport_lat, destination_airport_lon)),0) AS avg_km_travelled
FROM
  cleaned_data
GROUP BY
  user_id
  ),

--user personal information summary
user_personal_information AS(
  SELECT
  	DISTINCT (user_id), 
  	AGE(birthdate) AS age,
  	CASE 
      WHEN EXTRACT (YEAR FROM AGE(birthdate)) BETWEEN 18 AND 30 THEN 'Young'
      WHEN EXTRACT (YEAR FROM AGE(birthdate)) BETWEEN 31 AND 50 THEN 'Mid'
      ELSE 'Senior' 
  	END AS age_group,
  	gender,
  	married,
  	has_children,
  	home_country,
  	home_city,
  	home_airport,
  	home_airport_lat,
  	home_airport_lon,
	  sign_up_date
FROM
  	cleaned_data
  ),
  
/* 
The following query categorise users based on flight and hotel spend analysis. 
*/

hotel_flight_rank AS(
SELECT 
  user_id,
  total_hotel_spend,
  total_flight_spend,
  PERCENT_RANK() OVER (ORDER BY total_hotel_spend) AS hotel_spend_percentile,
  PERCENT_RANK() OVER (ORDER BY total_flight_spend) AS flight_spend_percentile,
  -- Categorizing Hotel Spend
  CASE 
    WHEN PERCENT_RANK() OVER (ORDER BY total_hotel_spend) >= 0.90 THEN 'High'
    WHEN PERCENT_RANK() OVER (ORDER BY total_hotel_spend) >= 0.75 THEN 'Upper Mid'
    WHEN PERCENT_RANK() OVER (ORDER BY total_hotel_spend) >= 0.25 THEN 'Mid'
    WHEN PERCENT_RANK() OVER (ORDER BY total_hotel_spend) >= 0.10 THEN 'Lower Mid'
    ELSE 'Low'
  END AS hotel_spend_category,
  -- Categorizing Flight Spend
  CASE 
    WHEN PERCENT_RANK() OVER (ORDER BY total_flight_spend) >= 0.90 THEN 'High'
    WHEN PERCENT_RANK() OVER (ORDER BY total_flight_spend) >= 0.75 THEN 'Upper Mid'
    WHEN PERCENT_RANK() OVER (ORDER BY total_flight_spend) >= 0.25 THEN 'Mid'
    WHEN PERCENT_RANK() OVER (ORDER BY total_flight_spend) >= 0.10 THEN 'Lower Mid'
    ELSE 'Low'
  END AS flight_spend_category,
  --travel frequency
  CASE 
      WHEN num_trips < 3 THEN 'Low'
      WHEN num_trips BETWEEN 3 AND 6 THEN 'Medium'
      WHEN num_trips > 6 THEN 'High'
    END AS travel_frequency,
  --booking behaviour
  CASE 
      WHEN avg_booking_days_in_advance < 7 THEN 'Last-minute'
      WHEN avg_booking_days_in_advance BETWEEN 7 AND 30 THEN 'Medium-term'
      WHEN avg_booking_days_in_advance > 30 THEN 'Long-term'
    END AS booking_behaviour
  
FROM user_travel_summary
  ),

/* The following queries calculates the proportion of discounted flights and hotels for each user. */
--discounted_flight_bookings

discounted_flight_bookings AS(
SELECT 
	user_id,
  COUNT(CASE WHEN flight_discount IS TRUE AND flight_booked IS TRUE THEN 1 END)/
  COUNT(CASE WHEN flight_booked IS TRUE THEN 1 END)::NUMERIC AS discounted_flight_booking_ratio
FROM cleaned_data
WHERE flight_booked IS TRUE 
GROUP BY user_id
),

--discounted_hotel_bookings

discounted_hotel_bookings AS(
SELECT 
	user_id,
	COUNT(CASE WHEN hotel_discount IS TRUE AND hotel_booked IS TRUE THEN 1 END)/
  COUNT(CASE WHEN hotel_booked IS TRUE THEN 1 END)::NUMERIC AS discounted_hotel_booking_ratio
FROM cleaned_data
WHERE hotel_booked IS TRUE
GROUP BY user_id
),
  
/* The following query generates all the important features. */
--feature table

features_table AS(  
SELECT 
  uss.user_id,
  uss.num_sessions,
  uss.avg_session_duration_sec,
  uss.avg_click_per_session,
  uts.num_trips,
  uts.num_flights,
  uts.total_seats,
 	uts.avg_seats,
	uts.total_flight_spend,
 	uts.avg_flight_spend,
  uts.total_hotel_spend,
  uts.avg_hotel_spend,
 	uts.avg_checked_bags,
  uts.avg_booking_days_in_advance,
	uts.total_km_flown,
  uts.avg_km_travelled,
  upi.age, 
  upi.age_group,
  upi.gender,
  upi.married,
  upi.has_children,
  CASE 
  		WHEN avg_flight_spend = 0 AND avg_hotel_spend = 0 THEN 'No traveller'		
  		WHEN avg_flight_spend =0 AND avg_hotel_spend >0 THEN 'Only hotel booking'
  		WHEN avg_flight_spend >0 AND avg_hotel_spend = 0 THEN 'Only flight booking'
  		WHEN avg_seats <= 2 AND avg_flight_spend > 0 AND avg_hotel_spend >0 THEN 'Solo' 
    	WHEN avg_seats >=2 AND married IS TRUE AND has_children IS TRUE THEN 'Family'
    	WHEN avg_seats >=2 AND married IS FALSE AND has_children IS TRUE THEN 'Family'
    	WHEN avg_seats >=2 AND married IS TRUE AND has_children IS FALSE THEN 'Couple'
    	WHEN avg_seats >=2 AND married IS FALSE AND has_children IS FALSE THEN 'Couple'
  END AS traveller_type,
  upi.home_country,
  upi.home_city,
  upi.home_airport,
  upi.home_airport_lat,
  upi.home_airport_lon,
  upi.sign_up_date,
  hfr.hotel_spend_category,
  hfr.flight_spend_category,
  hfr.travel_frequency,
  hfr.booking_behaviour,
  uid.num_international_flights,
  uid.num_domestic_flights,
  dfb.discounted_flight_booking_ratio,
  dhb.discounted_hotel_booking_ratio
FROM 
  users_session_summary uss
  LEFT JOIN user_travel_summary uts ON uss.user_id = uts.user_id
  LEFT JOIN user_personal_information upi ON uss.user_id = upi.user_id
  LEFT JOIN hotel_flight_rank hfr ON uss.user_id = hfr.user_id
  LEFT JOIN user_international_domestic uid ON uss.user_id = uid.user_id
  LEFT JOIN discounted_flight_bookings dfb ON uss.user_id = dfb.user_id
  LEFT JOIN discounted_hotel_bookings dhb ON uss.user_id = dhb.user_id
),

/* The following query generates a final features table that may be used for segmenting users. */
--final features_table
final_features_table AS (
SELECT 
	user_id, 
  gender,
  age_group,
  traveller_type,
  num_trips,
  travel_frequency,
  avg_checked_bags,
  booking_behaviour,
  flight_spend_category,
  hotel_spend_category,
  num_international_flights,
  num_domestic_flights,
  discounted_flight_booking_ratio,
  discounted_hotel_booking_ratio

FROM 
  features_table
),

/* The following query segments customers according to their behaviour taking
hotel spend, flight spend, traveller type, discount behaviour. */
--customer segmentation

customer_segmentation AS (
  SELECT
  	user_id, 
  	gender,
  	age_group,
 	 	traveller_type,
  	num_trips,
  	avg_checked_bags,
  	booking_behaviour,
  	flight_spend_category,
  	hotel_spend_category,
  	travel_frequency,
  	num_international_flights,
  	num_domestic_flights,
  	discounted_flight_booking_ratio,
  	discounted_hotel_booking_ratio, 
  	CASE 
  		--casual web searcher with no travel history
  		WHEN traveller_type = 'No traveller' 
  					THEN 'Casual web searcher' --condition 1, no taravel no perks
  		
  		-- Very high value customers with high spend on flight, hotels and frequent travel
  		WHEN (flight_spend_category IN ('High', 'Upper Mid') 
            	AND hotel_spend_category IN ('High', 'Upper Mid')
            	AND travel_frequency IN ('High', 'Medium')) 
  					THEN 'Platinum'-- condition 1, high spend and high travel frequency for appreciation,
  																																		 -- and upper mid for promotion
  		--
  		WHEN 	(booking_behaviour IN ('Last-minute') 
                AND traveller_type IN('Solo') 
                AND travel_frequency IN ('High', 'Medium')) --condition 1, possibly business traveller, appreciation
  					OR (flight_spend_category IN ('High', 'Upper Mid') 
             		AND hotel_spend_category IN ('High', 'Upper Mid')
            		AND travel_frequency IN ('Low')) --condition 2, promotion for high spend and low travel frequency
  					OR (traveller_type = 'Only flight booking') 
  							AND travel_frequency IN ('High', 'Medium') --condition 3, promotion
  					OR (traveller_type = 'Only hotel booking') 
  							AND travel_frequency IN ('High', 'Medium')--condition 4, promotion
  					THEN 'Gold'
  		
  		WHEN 	avg_checked_bags >1 --condition 1, heavy baggage taveller, promotion
  					OR (booking_behaviour IN ('Medium-term', 'Long-term') 
                AND travel_frequency IN ('High','Medium')) --booking behaviour for planned customer and high travel frequency, apreciation & promotion
  					THEN 'Silver'	
  		
  		WHEN (booking_behaviour IN ('Medium-term', 'Long-term') 
          		AND ((hotel_spend_category IN ('Mid', 'Lower Mid') 
            	OR flight_spend_category IN ('Mid', 'Lower Mid')))) -- Condition 1, planned travel but less spending, appreciation for free cancellation
    				OR (hotel_spend_category IN ('Mid', 'Upper Mid', 'High') 
        			AND flight_spend_category IN ('Mid', 'Lower Mid', 'Low')
        			AND travel_frequency IN ('High','Medium')) -- Condition 2, spending high on hotel but low in flight and high travel frequency, promotion for improving spend in other category
    				OR (flight_spend_category IN ('Mid', 'Lower Mid') 
        			AND hotel_spend_category IN ('Mid', 'Lower Mid')
        			AND travel_frequency IN ('High','Medium')) -- Condition 3, spending high in flights but low on hotels and high travel frequency, promotion for improving spend in other category
  					OR (booking_behaviour IN ('Last-minute') 
  							AND travel_frequency = 'Medium') --condition 4, last minute traveller with medium travel frequency, promotion for increasing travel frequency
  					THEN 'Bronze'

  		WHEN 	((discounted_flight_booking_ratio >= 0.1 OR discounted_hotel_booking_ratio >= 0.1)
  						AND (flight_spend_category IN ('Low', 'Lower Mid', 'Mid') OR hotel_spend_category IN ('Low', 'Lower Mid','Mid'))
  						) --condition 1, dicount traveller, low hotel and flight spending, appreciation
  					OR (traveller_type = 'Only hotel booking' 
                AND (travel_frequency = 'Low' 
                     OR hotel_spend_category IN ('Low', 'Lower Mid','Mid'))) --condition 2, promotion for low frequency traveller and low spending
						OR (traveller_type = 'Only flight booking' 
                AND (travel_frequency = 'Low' 
                     OR flight_spend_category IN ('Low', 'Lower Mid','Mid')))--condition 3, promotion for low frequency traveller and low spending
  					OR (booking_behaviour IN ('Last-minute') 
  						AND travel_frequency ='Low') --condition 4, promotion for low frequency traveller and low spending
  					THEN 'Blue'

  		ELSE 'No defined segment'
  	END AS customer_segment
  	 
  FROM 
  	final_features_table
),

/* The following query assigns perks to each customer. */

assigned_perks AS(
SELECT
	user_id,
  customer_segment,
  CASE
  	WHEN customer_segment = 'Platinum' THEN '1 night free hotel with flight'
    WHEN customer_segment = 'Gold' THEN 'Free hotel meal'
		WHEN customer_segment = 'Silver' THEN 'Free checked bag'
    WHEN customer_segment = 'Bronze' THEN 'No cancellation fees'
    WHEN customer_segment = 'Blue' THEN 'Exclusive discounts'	
  	WHEN customer_segment = 'Casual web searcher' THEN 'No perks'
  END AS customer_perk
FROM	
	customer_segmentation
  )
--Following queries helps to validate and test some conditions.

--a check for the final table
/*
SELECT *
FROM assigned_perks ;
*/

--query for customer segmnent, features and assigned perks
SELECT 
	ap.user_id,
  ap.customer_segment,
  ap.customer_perk,
  cs.gender,
  cs.age_group,
  cs.traveller_type,
  cs.num_trips,
  cs.avg_checked_bags,
  cs.booking_behaviour,
  cs.flight_spend_category,
  cs.hotel_spend_category,
  cs.travel_frequency,
  cs.discounted_flight_booking_ratio,
  cs.discounted_hotel_booking_ratio
 
FROM 
	assigned_perks ap
	LEFT JOIN customer_segmentation cs ON ap.user_id = cs.user_id
  ;

/*
SELECT *
FROM customer_segmentation
WHERE customer_segment = 'No defined segment'
*/

--Validate total number of distinct users in assigned perks table. It should be 5998
/*
SELECT COUNT(DISTINCT user_id)
FROM assigned_perks ;
*/

--verify the names of customer_segment, customer_perk and the counts
/*
SELECT 
	customer_segment,
  customer_perk AS perk,
  COUNT(*) AS num_of_customers
FROM
	assigned_perks
GROUP BY
	customer_segment,
  customer_perk ;
*/

--validate NO user is left without a perk assigned
/*
SELECT user_id
FROM assigned_perks
WHERE customer_perk IS NULL ;
*/

--validate ONE and ONLY ONE perk is assigned to each user
/*
SELECT *
FROM assigned_perks
GROUP BY user_id, customer_segment, customer_perk
HAVING COUNT(customer_perk) >1 ;
*/

--validate that no customer is left without a defined segment
/*
SELECT *
FROM customer_segmentation
WHERE customer_segment = 'No defined segment' ;
*/

/*
SELECT *
FROM customer_segmentation
WHERE customer_segment IS NULL ;
*/ 

--Please note the above two queries can also be merged in single WHERE clause as 
--WHERE customer_segment IS NULL OR customer_segment = 'No defined segment' :
/*
SELECT *
FROM customer_segmentation
WHERE customer_segment IS NULL OR customer_segment = 'No defined segment' ;
*/

/*
SELECT DISTINCT(traveller_type), COUNT(*)
FROM features_table
GROUP BY traveller_type
*/











