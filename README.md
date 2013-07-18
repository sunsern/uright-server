uright-server
=============
A lightweight server for uRight iOS app.


Requirements
============
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
./start-uright-server.py
</pre>


Monitoring training queue
=========================
<pre>
pip install rq-dashboard
rq-dashboard
</pre>

By default, the dashboard is at http://locahost:9181
