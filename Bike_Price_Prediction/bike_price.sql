# Create a database on Bike Price and use it going forward
drop database if exists bike_price_db;
create database bike_price_db;
use bike_price_db;

-- ---------------------------------------------------------------------------------------------
####### Table: Bike Price #######
create table bike_price(
	bike_id int primary key auto_increment,
    Brand varchar(100),
    Model varchar(100),
    Selling_Price float,
    year int,
    Seller_Type varchar(100),
    Owner varchar(100),
    KM_Driven int,
    Ex_Showroom_Price float
    );

# Show the table details    
desc bike_price;



# Check the details of the table
select * from bike_price;


    
    
