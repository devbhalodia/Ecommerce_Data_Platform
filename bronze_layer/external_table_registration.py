
%sql
CREATE SCHEMA IF NOT EXISTS ecom.bronze;


%sql
CREATE TABLE IF NOT EXISTS ecom.bronze.addresses USING DELTA LOCATION 's3://fivetran-landing-zone-devkb/neon-cdc/postgres_public/addresses/';
CREATE TABLE IF NOT EXISTS ecom.bronze.inventory USING DELTA LOCATION 's3://fivetran-landing-zone-devkb/neon-cdc/postgres_public/inventory/';
CREATE TABLE IF NOT EXISTS ecom.bronze.order_items USING DELTA LOCATION 's3://fivetran-landing-zone-devkb/neon-cdc/postgres_public/order_items';
CREATE TABLE IF NOT EXISTS ecom.bronze.orders USING DELTA LOCATION 's3://fivetran-landing-zone-devkb/neon-cdc/postgres_public/orders';
CREATE TABLE IF NOT EXISTS ecom.bronze.payments USING DELTA LOCATION 's3://fivetran-landing-zone-devkb/neon-cdc/postgres_public/payments';
CREATE TABLE IF NOT EXISTS ecom.bronze.products USING DELTA LOCATION 's3://fivetran-landing-zone-devkb/neon-cdc/postgres_public/products';
CREATE TABLE IF NOT EXISTS ecom.bronze.refunds USING DELTA LOCATION 's3://fivetran-landing-zone-devkb/neon-cdc/postgres_public/refunds';
CREATE TABLE IF NOT EXISTS ecom.bronze.shipments USING DELTA LOCATION 's3://fivetran-landing-zone-devkb/neon-cdc/postgres_public/shipments';
CREATE TABLE IF NOT EXISTS ecom.bronze.users USING DELTA LOCATION 's3://fivetran-landing-zone-devkb/neon-cdc/postgres_public/users';