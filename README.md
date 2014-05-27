Fullmatix (FulliAutomatix)
==========================

Collection of distributed systems built using Helix. There are quite a few systems that work very well on a single machine. The goal of FullMatix (FulliAutomatix) is to make them distributed. Not only distributed but the system will be dynamically configurable and provide a X as a Service model. First up is Databases - MySQL (could not think of a better one). 

Helix already has integration with YARN and all these projects will be deployed through YARN.

This is a fun project and none of these apart from Helix itself is deployed in PROD. If you want to try it in PROD, it will be my pleasure to help you.

mysql-cluster
------------------

Sharded, replicated, fault tolerant MySQL. Requires MySQL 5.6 +. 

More info: [mysql-cluster docs](mysql-cluster/README.md)


More to come
-------------

Distributed memcache - I have to do this :-)

Distributed Riemann - All components will need a single monitoring system

Distributed in-memory buffer 

Distributed task framework - This will probably be part of Helix itself and will be Production quality

Distributed queue: probably not, there are lot of good ones available out there.



