;;;; package.lisp

(defpackage #:cl-fbclient
  (:use #:cl #:cffi )
  (:export :fb-database
	   :fb-transaction
	   :fb-statement

	   :fb-connect
	   :fb-disconnect
	   :fb-connected-p

	   :fb-start-transaction
	   :fb-commit-transaction
	   :fb-rollback-transaction

	   :fb-allocate-statement
	   :fb-prepare-statement
	   :fb-execute-statement
	   :fb-prepare-and-execute-statement
	   :fb-statement-fetch
	   :fb-statement-free
	   :fb-statement-get-var-val
	   :fb-statement-get-vars-vals-list
	   :fb-statement-get-vars-names-list
	   :fb-statement-get-var-val+name
	   :fb-statement-get-vars-vals+names-list
	   :fb-statement-get-var-info
	   :fb-statement-get-vars-info
	   :fb-get-sql-type

	   :fb-with-database
	   :fb-with-transaction
	   :fb-with-statement
	   :fb-with-statement-db

	   :fb-loop-statement-fetch
	   :fb-loop-query-fetch

	   :fb-query

	   :fb-noresult-query
	   :fb-query-fetch-all
	   :fb-query-fetch-all+names
	   :fb-query-fetch-all+names-header

	   :fb-error-code
	   :fb-error-text
	   :fbclient-msg
	   :fb-error
	   :fb-verbalize-error

           :*timestamp-alist-converter*
           :timestamp-alist-to-string
	   
	   :fb-connect-toplevel
	   :fb-disconnect-toplevel
	   :fb-with-toplevel-connection
	   :*database-toplevel*

	   :fb-blob-open
	   :fb-blob-read
	   :fb-blob-close
	   :fb-blob-load
	   :*fb-blob-read-block-size*
	   :*blob-converter*
	   :fb-blob-to-string-converter
	   ))
