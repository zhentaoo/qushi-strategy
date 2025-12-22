
from requests import api
import api_core

print("Testing cancel_all_trailing_stop_orders...")

# res = api_core.place_trailing_stop_order('SOLUSDT', 0.06, 2.0)



api_core.cancel_all_trailing_stop_orders()