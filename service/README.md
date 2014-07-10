API Service description

API service for browsing datasets functionality is complete, now it's possible to integrate all backend functionality with Drupal frontend. This service produces JSON for transfer data between frontend and backend so Gaele can use it to extend functionality of Drupal websites with advanced features.

Entry points are:
API service
http://node-146.dev.socialhistoryservices.org/service
Description of the service

http://node-146.dev.socialhistoryservices.org/service/regions
Get list of regions. Variables:
- region_id
- region_name
- region_description 
- region_code
- region_ord 
- active
- region_year

Example:
http://node-146.dev.socialhistoryservices.org/service/regions?region_id=2

http://node-146.dev.socialhistoryservices.org/service/topics
Get list of topics. Variables are:
- topic_id
- datatype
- topic_name
- topic_root

It's possible to make filter on any variable name in json, for example:
http://node-146.dev.socialhistoryservices.org/service/topics?topic_root=6
or
http://node-146.dev.socialhistoryservices.org/service/topics?topic_id=2

http://node-146.dev.socialhistoryservices.org/service/data
Data query node

http://node-146.dev.socialhistoryservices.org/service/histclasses
The list of historical classes

http://node-146.dev.socialhistoryservices.org/service/years
The list of years

http://node-146.dev.socialhistoryservices.org/service/maps
API with poligon points to visualize historical maps

Note: regions and historical classes are in Russian language so service will produce unicode output.
