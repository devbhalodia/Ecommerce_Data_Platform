select * 
from {{ source('silver', 'users') }} 