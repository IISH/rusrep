## Russian Repository API Service 

API service for browsing datasets functionality is complete, now it's possible to integrate all backend functionality with Drupal frontend. This service produces JSON for transfer data between frontend and backend so Gaele can use it to extend functionality of Drupal websites with advanced features.

Entry points are:
API service
http://node-146.dev.socialhistoryservices.org/service

## API Description

<pre class="terminal">
$ curl -i http://node-146.dev.socialhistoryservices.org/service/topics
{
    "data": [
        {
            "datatype": "6", 
            "description": " ", 
            "topic_id": 1, 
            "topic_name": "CAPITAL", 
            "topic_root": 0
        }, 
        {
            "datatype": "6.01", 
            "description": " ", 
            "topic_id": 2, 
            "topic_name": "Capital assets", 
            "topic_root": 6
        }, 
        {
            "datatype": "6.02", 
            "description": " ", 
            "topic_id": 3, 
            "topic_name": "Investments", 
            "topic_root": 6
        }, 
        {
            "datatype": "6.03", 
            "description": " ", 
            "topic_id": 4, 
            "topic_name": "Interest", 
            "topic_root": 6
        }, 
        {
            "datatype": "2", 
            "description": " ", 
            "topic_id": 5, 
            "topic_name": "LABOUR", 
            "topic_root": 0
        }, 
        {
            "datatype": "2.01", 
            "description": " ", 
            "topic_id": 6, 
            "topic_name": "By profession", 
            "topic_root": 2
        }, 
        {
            "datatype": "2.03", 
            "description": " ", 
            "topic_id": 7, 
            "topic_name": "By sector of employment", 
            "topic_root": 2
        } 
}
</pre>
API output is a list of topics. Variables are:
- topic_id
- datatype
- topic_name
- topic_root

It's possible to make filter on any variable name in json, for example:
<pre class="terminal">
$ curl -i http://node-146.dev.socialhistoryservices.org/service/topics?topic_root=6
or
$ curl -i http://node-146.dev.socialhistoryservices.org/service/topics?topic_id=2
</pre>


<pre class="terminal">
$ curl -i http://node-146.dev.socialhistoryservices.org/service/regions
{
    "regions": [
        {
            "active": 1, 
            "region_code": "1897_97", 
            "region_description": "\u042f\u0440\u043e\u0441\u043b\u0430\u0432\u0441\u043a\u0430\u044f", 
            "region_id": 1, 
            "region_name": "\u042f\u0440\u043e\u0441\u043b\u0430\u0432\u0441\u043a\u0430\u044f", 
            "region_ord": 110301088
        }, 
        {
            "active": 1, 
            "region_code": "1897_96", 
            "region_description": "\u042f\u043a\u0443\u0442\u0441\u043a\u0430\u044f", 
            "region_id": 2, 
            "region_name": "\u042f\u043a\u0443\u0442\u0441\u043a\u0430\u044f", 
            "region_ord": 110301082
        }, 
        {
            "active": 1, 
            "region_code": "1897_95", 
            "region_description": "\u044d\u0441\u0442\u043b\u044f\u043d\u0434\u0441\u043a\u0430\u044f", 
            "region_id": 3, 
            "region_name": "\u044d\u0441\u0442\u043b\u044f\u043d\u0434\u0441\u043a\u0430\u044f", 
            "region_ord": 110101089
        }, 
        {
            "active": 1, 
            "region_code": "1897_94", 
            "region_description": "\u042d\u0440\u0438\u0432\u0430\u043d\u0441\u043a\u0430\u044f", 
            "region_id": 4, 
            "region_name": "\u042d\u0440\u0438\u0432\u0430\u043d\u0441\u043a\u0430\u044f", 
            "region_ord": 110101088
        }, 
        {
            "active": 1, 
            "region_code": "1897_93", 
            "region_description": "\u0427\u0435\u0440\u043d\u043e\u043c\u043e\u0440\u0441\u043a\u0430\u044f", 
            "region_id": 5, 
            "region_name": "\u0427\u0435\u0440\u043d\u043e\u043c\u043e\u0440\u0441\u043a\u0430\u044f", 
            "region_ord": 109501077
        }, 
        {
            "active": 1, 
            "region_code": "1897_92", 
            "region_description": "\u0447\u0435\u0440\u043d\u0438\u0433\u043e\u0432\u0441\u043a\u0430\u044f", 
            "region_id": 6, 
            "region_name": "\u0447\u0435\u0440\u043d\u0438\u0433\u043e\u0432\u0441\u043a\u0430\u044f", 
            "region_ord": 109501077
        }, 
        {
            "active": 1, 
            "region_code": "1897_91", 
            "region_description": "\u0425\u0435\u0440\u0441\u043e\u043d\u0441\u043a\u0430\u044f", 
            "region_id": 7, 
            "region_name": "\u0425\u0435\u0440\u0441\u043e\u043d\u0441\u043a\u0430\u044f", 
            "region_ord": 109301077
        }
}
</pre>
Returns list of regions. Variables:

	- region_id

	- region_name

	- region_description 

	- region_code

	- region_ord 

	- active

	- region_year

Example:
<pre class="terminal">
$ curl -i http://node-146.dev.socialhistoryservices.org/service/regions?region_id=2
</pre>

## Data query node
<pre class="terminal">
$ curl -i http://node-146.dev.socialhistoryservices.org/service/data
</pre>

## The list of historical classes
<pre class="terminal">
$ curl -i ttp://node-146.dev.socialhistoryservices.org/service/histclasses
</pre>

http://node-146.dev.socialhistoryservices.org/service/years
## The list of years
<pre class="terminal">
$ curl -i http://node-146.dev.socialhistoryservices.org/service/years
</pre>

## API with poligon points to visualize historical maps
<pre class="terminal">
$ curl -i http://node-146.dev.socialhistoryservices.org/service/maps
</pre>

Note: regions and historical classes are in Russian language so service will produce unicode output.
