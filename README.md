uright-server
=============
A lightweight backend server for uRight iOS app.


Requirements
============
* uright-python (https://github.com/sunsern/uright-python)
* redis (http://redis.io/)
* rq (http://python-rq.org/)
* Flask (http://flask.pocoo.org/docs/)

<pre>
apt-get install redis-server
pip install rq
pip install Flask
</pre>


Start the server
================
<pre>
service haproxy start
</pre>
<pre>
./start-uright-server.py
</pre>

Stop the server
================
<pre>
./start-uright-server.py
</pre>



Monitoring training queue
=========================
<pre>
pip install rq-dashboard
rq-dashboard
</pre>

By default, the dashboard is at http://locahost:9181
