ALTER TABLE dim_card_details 
ALTER COLUMN card_number TYPE VARCHAR(20)  USING card_number::varchar(20),
ALTER COLUMN expiry_date TYPE VARCHAR(6)  USING expiry_date::varchar(6),
ALTER COLUMN date_payment_confirmed TYPE DATE using date_payment_confirmed::date,
ADD PRIMARY KEY (card_number);