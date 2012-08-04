cl-fbclient
===========
Common Lisp library for working with firebird databases
-----------
Library is a set of classes and methods for working with firebird databases.
Basic classes:
- fb-database
- fb-transaction
- fb-statement
- fb-error

Supported SQL-vars types:
- float
- double
- integer
- varchar
- timestamp

-----------
**Example:**
<pre>
(require 'cl-fbclient)

;; create an instance of the database and automatically connect to the database
(defparameter *db* (make-instance 'cl-fbclient:fb-database
  				   :path "/path-to-db/db-file.fdb"))
             
;; query that returns no value
;; (transaction will be created, started and commited automatically)
(cl-fbclient:fb-noresult-query *db* "INSERT INTO T1(A1,A2) VALUES(121, 42)")

;; to query and write results to the list
;; (transaction will be created, started and commited automatically)
(cl-fbclient:fb-query-fetch-all *db* "SELECT * FROM t1")

;; disconnecting from DB
(cl-fbclient:fb-disconnect *db*)
</pre>

**Tested on:**
  - SBCL(ubuntu)
  - SBCL-win32-threads (by akovalenko). (WindowsXP SP3)

Documentation: <a href="http://github.com/klimenko-serj/cl-fbclient/wiki">**cl-fbclient/wiki**</a>
---------------         

TODO:
---------------
- Create support types: date, time, array
- finalize documentation
- Add methods to get **Named** Lists of fetched vals like (:name-1 val-1 :name-2 val-2 ...
- ...

