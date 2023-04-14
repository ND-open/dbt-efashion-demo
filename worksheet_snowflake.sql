-- Etape 1 : Création d'une nouvelle base de données et d'un rôle associé
use role sysadmin;
create or replace database efashion;

use efashion;
create or replace schema stg_bronze;

-- Etape 2 : Lier Snowflake avec un Azure Cloud Storage
-- https://docs.snowflake.com/en/user-guide/data-load-azure-config
create or replace storage integration azure_nd
type = external_stage
storage_provider  = 'AZURE'
enabled = true
azure_tenant_id = 'd7c9c94d-28a2-4298-a72e-b1bee01d5b58'
storage_allowed_locations = ('azure://blobstorenextdecision.blob.core.windows.net/efashion/');

-- Pour pouvoir autoriser :
desc storage integration azure_nd;

-- Travail côté Azure pour ajouter des droits au compte Snowflake

-- Etape 3 : Création du stage Externe
use schema efashion.stg_bronze;

-- Création du file format
create or replace file format csv
 type = csv
 field_delimiter = ','
 field_optionally_enclosed_by = '"'
 skip_header = 1;

-- Création du stage externe
create or replace stage ext_efashion
 storage_integration = azure_nd
 url = 'azure://blobstorenextdecision.blob.core.windows.net/efashion/efashion.csv'
 file_format = csv;

 -- Voir le contenu du stage externe
list @ext_efashion;

-- Lecture du contenu du stage externe
select 
 t.$1,
 t.$2,
 t.$3,
 t.$4,
 t.$5,
 t.$6,
 t.$7,
 t.$8,
 t.$9,
 t.$10
from @ext_efashion (file_format => csv) t;

-- utilisation de la clause COPY INTO
create or replace table raw_efashion(
    year varchar(4),
    quarter varchar(2),
    month smallint,
    state varchar(50),
    city varchar(80),
    store_name varchar(80),
    lines varchar(80),
    category varchar(150),
    sales_revenue decimal(15,2),
    quantity_sold int
);

copy into raw_efashion from @ext_efashion file_format = (format_name = 'csv');

select * from raw_efashion;
