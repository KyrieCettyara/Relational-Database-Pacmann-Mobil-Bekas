-- City Table
CREATE TABLE cities (
  city_id SERIAL PRIMARY KEY,
  city_name VARCHAR(50) NOT NULL,
  latitude FLOAT NOT NULL,
  longitude FLOAT NOT NULL
);

-- Car Table
CREATE TABLE cars (
  car_id SERIAL PRIMARY KEY,
  brand VARCHAR(100) NOT NULL,
  model VARCHAR(255) NOT NULL,
  car_type VARCHAR(25) NOT NULL,
  year INT NOT NULL,
  price INT NOT NULL
);

-- Users
CREATE TABLE users (
  user_id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  contact VARCHAR(20) NOT NULL,
  city_id INT NOT NULL,
  FOREIGN KEY (city_id) REFERENCES cities(city_id)
);

-- Advertisement Table
CREATE TABLE advertisement (
  adv_id SERIAL NOT NULL PRIMARY KEY,
  car_id integer NOT NULL,
  user_id integer NOT NULL,
  color text NOT NULL,
  mileage_in_km integer NOT NULL,
  bid_flag boolean NOT NULL,
  post_date date NOT NULL,
  FOREIGN KEY (car_id) REFERENCES cars(car_id),
  FOREIGN KEY (user_id) REFERENCES users(user_id)
);


-- Bid Table
CREATE TABLE bids( 
  bid_id SERIAL PRIMARY KEY,
  adv_id integer NOT NULL,
  bidder_id INT NOT NULL,
  bid_date DATE NOT NULL,
  bid_price INT NOT NULL,
  FOREIGN KEY (adv_id) REFERENCES advertisement(adv_id),
  FOREIGN KEY (bidder_id) REFERENCES users(user_id)
);


--transactional query 1
SELECT *
FROM cars
WHERE year >= 2015;


--	transactional query 2
INSERT INTO bids
	(bid_id,adv_id, bidder_id, bid_date, bid_price)
VALUES
	(DEFAULT, 1, 1, '2023-09-07',  100000000);


--	transactional query 3
SELECT cars.car_id, cars.brand, cars.model, cars.price, adv.post_date
FROM advertisement adv
LEFT JOIN users using (user_id)
LEFT JOIN cars using (car_id)
where users.name = 'Farhunnisa Putra, S.E.'
order by adv.post_date desc


--	transactional query 4	
SELECT car_id, brand, model, year, price
FROM cars
WHERE model LIKE '%Yaris'
ORDER BY price ASC;


--	transactional query 5
SELECT cars.car_id
	, cars.brand
	, cars.model
	, cars.year
	, cars.price
FROM cars
	JOIN advertisement  USING (car_id)
	JOIN users USING (user_id)
	JOIN cities ON cities.city_id = users.city_id
WHERE cities.city_id = 3171
ORDER BY SQRT(POW(cities.latitude - (-6.186486), 2) + POW(cities.longitude - 106.834091, 2));


-- analytic query 1
SELECT cars.model
	, COUNT(cars.car_id) AS count_product
	, COUNT(bids.bid_id) AS count_bid
FROM cars
LEFT JOIN advertisement USING (car_id)
LEFT JOIN bids  USING (adv_id)
GROUP BY cars.model
ORDER BY count_bid DESC;


-- analytic query 2
WITH avg_per_city as
	(
		SELECT cities.city_id
			, AVG(cars.price) AS avg_price
		FROM users 
			JOIN advertisement adv USING (user_id)
			JOIN cities USING (city_id)
			JOIN cars ON adv.car_id = cars.car_id
		GROUP BY cities.city_id
	)

SELECT cities.city_name
	, cars.brand
	, cars.model
	, cars.year
	, cars.price
	, apc.avg_price
FROM users 
JOIN advertisement adv USING (user_id)
JOIN cities USING (city_id)
JOIN cars ON adv.car_id = cars.car_id
JOIN avg_per_city apc USING (city_id)

-- analytic query 3
CREATE OR REPLACE VIEW cars_bids AS
SELECT
	cars.model
	, bids.bidder_id
	, bids.bid_date
	, bid_price
FROM cars
	JOIN advertisement adv USING (car_id)
	JOIN bids ON adv.adv_id = bids.adv_id
WHERE cars.model LIKE 'Toyota Yaris' 
ORDER BY user_id
	, bid_date ASC;

SELECT model
	, bidder_id
	, bid_date
	, bid_price
	, LEAD(bid_date) OVER (PARTITION BY bidder_id ORDER BY bid_date) as next_bid_date
	, LEAD(bid_price_idr) OVER (PARTITION BY bidder_id ORDER BY bid_date) as next_bid_price_idr
FROM yaris_bids
ORDER BY yaris_bids.user_id ASC, bid_date ASC;


-- analytic query 4
WITH model_bid_int_6m AS
	(
		SELECT cars.model
			, bids.bid_price
			, bids.bid_date
		FROM bids
			LEFT JOIN advertisement USING (adv_id)
			LEFT JOIN cars USING (car_id)
		WHERE bid_date >= NOW() - INTERVAL '6 months'
)

SELECT cars.model
	, AVG(bids.bid_price) AS avg_price
	, AVG(avg_bid.bid_price) AS avg_bid_6month
	, AVG(bids.bid_price) - AVG(avg_bid.bid_price) AS difference
	, ((AVG(bids.bid_price) - AVG(avg_bid.bid_price))/ AVG(bids.bid_price))*100 AS difference_percent
FROM bids
	LEFT JOIN advertisement USING (adv_id)
	LEFT JOIN cars USING (car_id)
	LEFT JOIN model_bid_int_6m avg_bid ON avg_bid.model = cars.model
GROUP BY cars.model;





