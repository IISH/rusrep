API Service description

API service for browsing datasets functionality is complete, now it's possible to integrate all backend functionality with Drupal frontend. This service produces JSON for transfer data between frontend and backend so Gaele can use it to extend functionality of Drupal websites with advanced features.

Entry points are:
1. API service
http://node-146.dev.socialhistoryservices.org/service
Description of the service

2. http://node-146.dev.socialhistoryservices.org/service/regions
List of regions

3. http://node-146.dev.socialhistoryservices.org/service/topics
List of topics

4. http://node-146.dev.socialhistoryservices.org/service/data
Data query node

5. http://node-146.dev.socialhistoryservices.org/service/histclasses
The list of historical classes

6. http://node-146.dev.socialhistoryservices.org/service/years
The list of years

7. API with poligon points to visualize historical maps:
http://node-146.dev.socialhistoryservices.org/service/maps

Note: regions and historical classes are in russian so service will produce unicode output.
