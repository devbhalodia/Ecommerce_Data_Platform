select * 
from {{ source('silver', 'shipments') }} 