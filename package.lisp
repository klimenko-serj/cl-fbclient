;;;; package.lisp

(defpackage #:cl-fbclient
  (:use #:cl #:cffi)
  (:export :fb-database
	   :fb-transaction
	   :fb-statement
	   :fb-connect
	   :fb-disconnect
	   :fb-start-transaction
	   :fb-commit-transaction
	   :fb-rollback-transaction
	   :fb-prepare-and-execute-statement
	   :fb-statement-fetch
	   :fb-statement-free
	   :fb-statement-get-var-val
	   :fb-statement-get-vars-vals-list
	   :fb-get-sql-type
	   :fb-with-transaction
	   :fb-with-statement
	   :fb-with-statement-db
	   :fb-loop-statement-fetch
	   :fb-loop-query-fetch
	   :fb-noresult-query
	   :fb-query-fetch-all
	   :fb-error-code
	   :fb-error-text
	   :fbclient-msg
	   ;:fbclient-dsql-msg
	   :fb-error
	   ;:fb-dsql-error
	   :fb-verbalize-error
	   ))

