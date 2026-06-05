select * 
from {{ source('silver', 'addresses') }} 