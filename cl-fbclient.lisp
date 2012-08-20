;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; cl-fbclient.lisp
;;;; CONDITIONS and CLASSES for "cl-fbclient"
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(in-package #:cl-fbclient)
;===================================================================================
;; GENERICS
;-----------------------------------------------------------------------------------
(defgeneric fb-verbalize-error (err)
  (:documentation "The method, which creates a single text error message."))
;(defgeneric fb-connect (fb-db) 
;  (:documentation "Method to connect to the database."))
;(defgeneric fb-disconnect (db)
;  (:documentation "Method to disconnect from the database."))
;; (defgeneric fb-start-transaction (transaction)
;;   (:documentation "Method to start transaction."))
;; (defgeneric fb-commit-transaction (transaction)
;;   (:documentation "Method to commit transaction."))
;; (defgeneric fb-rollback-transaction (transaction)
;;   (:documentation "Method to rollback transaction."))
;; (defgeneric fb-prepare-and-execute-statement (statement)
;;   (:documentation "Method to prepare and execute statement."))
;; (defgeneric fb-statement-free (statement)
;;   (:documentation "Method to free statement."))
;; (defgeneric fb-statement-fetch (statement)
;;   (:documentation "Method to fetch results from executed statement."))
;; (defgeneric fb-statement-get-var-val (statement index)
;;   (:documentation "A method for obtaining the values of result variables. Used after Fetch."))
;; (defgeneric fb-statement-get-vars-vals-list (statement)
;;   (:documentation "A method for obtaining the list of values ​​of result variables. Used after Fetch."))
;; (defgeneric fb-statement-get-var-val+name (statement index)
;;   (:documentation "A method for obtaining the values and names of result variables. Used after Fetch."))
;; (defgeneric fb-statement-get-vars-vals+names-list (statement)
;;   (:documentation "A method for obtaining the list of values and names of result variables. Used after Fetch."))
;; (defgeneric fb-statement-get-vars-names-list (statement)
;;   (:documentation "A method for obtaining names of result variables. Used after Fetch."))
;; (defgeneric fb-noresult-query (fb-db request-str)
;;   (:documentation "A method for performing queries that do not require answers.(insert,delete,update, etc.) 
;; (transaction will be created, started and commited automatically)"))
;; (defgeneric fb-query-fetch-all (fb-db request-str)
;;   (:documentation "The method, which executes the query and returns all its results in a list. 
;; (transaction will be created, started and commited automatically)"))
;; (defgeneric fb-query-fetch-all+names (fb-db request-str)
;;   (:documentation "The method, which executes the query and returns all its results(+names) in a list. 
;; (transaction will be created, started and commited automatically)"))
;; (defgeneric fb-query-fetch-all+names-header (fb-db request-str)
;;   (:documentation "The method, which executes the query and returns all its results(+names header) in a list. 
;; (transaction will be created, started and commited automatically)"))
;===================================================================================
;; PARAMETERS
;-----------------------------------------------------------------------------------
;; *convert-timestamp-to-string* - defined in "cl-fbclient-functions.lisp"
;; *timestamp-string-format* - defined in "cl-fbclient-functions.lisp"
;===================================================================================
;; FB-ERROR
;-----------------------------------------------------------------------------------
(define-condition fb-error (error)
  ((fb-error-code :initarg :fb-error-code :reader fb-error-code)
   (fb-error-text :initarg :fb-error-text :reader fb-error-text)
   (fbclient-msg :initarg :fbclient-msg :reader fbclient-msg))
  (:documentation "Condition for processing fbclient errors."))
;-----------------------------------------------------------------------------------
(defmethod fb-verbalize-error ((err fb-error))
  (format nil "!fb-error:~%~tcode: ~a~%~ttext: ~a~%~tfbclient-msg: ~a" 
	  (cl-fbclient:fb-error-code err)
	  (cl-fbclient:fb-error-text err)
	  (cl-fbclient:fbclient-msg err)))
;===================================================================================
;; FB-DATABASE
;-----------------------------------------------------------------------------------
(defclass fb-database ()
  ((db-handle* :accessor db-handle*)
   (host :accessor host
	 :initarg :host
	 :initform "localhost")
   (path :accessor path
	 :initarg :path)
   (user-name :accessor user-name
	      :initarg :user-name
	      :initform "SYSDBA")
   (password :accessor password
	     :initarg :password
	     :initform "masterkey"))
  (:documentation "Class that handles database connection"))
;-----------------------------------------------------------------------------------
(defun fb-connect (fb-db)
  "Method to connect to the database."
  (let ((host+path (concatenate 'string (host fb-db) ":" (path fb-db)))
	(status-vector* (make-status-vector)))
    (connect-to-db (db-handle* fb-db) status-vector* host+path (user-name fb-db) (password fb-db))
    (unwind-protect 
	 (when (status-vector-error-p status-vector*)
	   (error 'fb-error 
		  :fb-error-code 10 
		  :fb-error-text (format nil "Unable to connect ('~a')" host+path)
		  :fbclient-msg (get-status-vector-msg status-vector*)))
    (cffi-sys:foreign-free status-vector*))))
;-----------------------------------------------------------------------------------
(defmethod initialize-instance :after ((db fb-database) &key (no-auto-connect Nil))
  (progn (setf (db-handle* db) (make-db-handler))
	 (when (null no-auto-connect) (fb-connect db))))
;-----------------------------------------------------------------------------------
(defun fb-disconnect (db)
  "Method to disconnect from the database."
  (let ((status-vector* (make-status-vector)))
    (isc-detach-database status-vector* (db-handle* db))
    (unwind-protect 
	 (when (status-vector-error-p status-vector*)
	   (error 'fb-error 
		  :fb-error-code 11 
		  :fb-error-text "Error when disconnecting from DB"
		  :fbclient-msg (get-status-vector-msg status-vector*)))
    (cffi-sys:foreign-free status-vector*))))
;-----------------------------------------------------------------------------------
;; FB-TRANSACTION
;-----------------------------------------------------------------------------------
(defclass fb-transaction ()
  ((fb-db :accessor fb-db
	  :initarg :fb-db)
   (transaction-handle* :accessor transaction-handle*))
  (:documentation "Class that handles transaction."))
;-----------------------------------------------------------------------------------
(defun fb-start-transaction (tr)
  "Method to start transaction."
  (let ((status-vector* (make-status-vector)))
    (start-transaction (db-handle* (fb-db tr)) (transaction-handle* tr) status-vector*)
    (unwind-protect 
	 (when (status-vector-error-p status-vector*)
	   (error 'fb-error 
		  :fb-error-code 20
		  :fb-error-text "Unable to start transaction"
		  :fbclient-msg (get-status-vector-msg status-vector*)))
      (cffi-sys:foreign-free status-vector*))))
;-----------------------------------------------------------------------------------
(defmethod initialize-instance :after ((tr fb-transaction) &key (no-auto-start Nil))
  (progn (setf (transaction-handle* tr) (make-tr-handler))
	 (when (null no-auto-start) (fb-start-transaction tr))))
;-----------------------------------------------------------------------------------
(defun fb-commit-transaction (tr)
  "Method to commit transaction."
  (let ((status-vector* (make-status-vector)))
    (isc-commit-transaction status-vector* (transaction-handle* tr))
    (unwind-protect 
	 (when (status-vector-error-p status-vector*)
	   (error 'fb-error 
		  :fb-error-code 21 
		  :fb-error-text "Unable to commit transaction"
		  :fbclient-msg (get-status-vector-msg status-vector*)))
    (cffi-sys:foreign-free status-vector*))))
;-----------------------------------------------------------------------------------
(defun fb-rollback-transaction (tr)
  "Method to rollback transaction."
  (let ((status-vector* (make-status-vector)))
    (isc-rollback-transaction status-vector* (transaction-handle* tr))
    (unwind-protect 
	 (when (status-vector-error-p status-vector*)
	   (error 'fb-error 
		  :fb-error-code 22
		  :fb-error-text "Unable to rollback transaction"
		  :fbclient-msg (get-status-vector-msg status-vector*)))
      (cffi-sys:foreign-free status-vector*))))
;-----------------------------------------------------------------------------------
;; FB-STATEMENT
;-----------------------------------------------------------------------------------
(defclass fb-statement ()
  ((fb-tr :accessor fb-tr
	  :initarg :fb-tr)
   (request-str :accessor request-str
		:initarg :request-str)
   (statement-handle* :accessor statement-handle*
		      :initform (make-stmt-handler))
   (xsqlda-output* :accessor xsqlda-output*
		   :initform nil)
   (st-type :writer (setf st-type)
	    :reader fb-get-sql-type
	 :initform Nil))
  (:documentation "Class that handles SQL statements."))
;-----------------------------------------------------------------------------------
(defun fb-allocate-statement (fb-stmt)
  "Method to allocate statement."
  (let ((status-vector* (make-status-vector)))
    (isc-dsql-allocate-statement status-vector*
				 (db-handle* (fb-db (fb-tr fb-stmt)))
				 (statement-handle* fb-stmt))
    (when (status-vector-error-p status-vector*)
      (unwind-protect
	   (error 'fb-error 
		  :fb-error-code 30 
		  :fb-error-text "Unable to allocate statement"
		  :fbclient-msg (get-status-vector-msg status-vector*))
	(cffi-sys:foreign-free status-vector*)))
    (cffi-sys:foreign-free status-vector*)))
;-----------------------------------------------------------------------------------
(defun fb-prepare-statement (fb-stmt)
  "Method to prepare statement."
  (let ((status-vector* (make-status-vector)))
    (isc-dsql-prepare status-vector*
		      (transaction-handle* (fb-tr fb-stmt))
		      (statement-handle* fb-stmt)
		      0 
		      (cffi:foreign-string-alloc (request-str fb-stmt)) 
		      0 
		      (cffi:null-pointer))
    (when (status-vector-error-p status-vector*)
      (unwind-protect
	   (error 'fb-error 
		  :fb-error-code 31 
		  :fb-error-text (format nil "Unable to prepare statement: ~a"
					 (request-str fb-stmt))
		  :fbclient-msg (get-status-vector-msg status-vector*))
	(cffi-sys:foreign-free status-vector*)))
    (setf (st-type fb-stmt) (get-sql-type (statement-handle* fb-stmt)))
    (setf (xsqlda-output* fb-stmt) (make-xsqlda 10))
    (isc-dsql-describe status-vector* 
		       (statement-handle* fb-stmt)
		       1 
		       (xsqlda-output* fb-stmt))
    (when (status-vector-error-p status-vector*)
      (unwind-protect
	   (error 'fb-error 
		  :fb-error-code 32 
		  :fb-error-text "Error in isc-dsql-describe"
		  :fbclient-msg (get-status-vector-msg status-vector*))
	(cffi-sys:foreign-free status-vector*)))
    (setf (xsqlda-output* fb-stmt) (remake-xsqlda (xsqlda-output* fb-stmt)))
    (alloc-vars-data (xsqlda-output* fb-stmt))
    (cffi-sys:foreign-free status-vector*)))
;-----------------------------------------------------------------------------------
(defun fb-execute-statement (fb-stmt)
  "Method to execute statement."
  (let ((status-vector* (make-status-vector)))
     (isc-dsql-execute status-vector*
		       (transaction-handle* (fb-tr fb-stmt))
		       (statement-handle* fb-stmt)
		       1 
		       (cffi:make-pointer 0))
     (when (status-vector-error-p status-vector*)
       (unwind-protect
	    (error 'fb-error 
		   :fb-error-code 33 
		   :fb-error-text "Unable to execute statement"
		   :fbclient-msg (get-status-vector-msg status-vector*))
	 (cffi-sys:foreign-free status-vector*)))
     (cffi-sys:foreign-free status-vector*)))
;-----------------------------------------------------------------------------------
(defun fb-prepare-and-execute-statement (fb-stmt)
  "Method to prepare and execute statement."
  (let ((status-vector* (make-status-vector)))
    (isc-dsql-allocate-statement status-vector*
				 (db-handle* (fb-db (fb-tr fb-stmt)))
				 (statement-handle* fb-stmt))
    
    (when (status-vector-error-p status-vector*)
      (unwind-protect
	   (error 'fb-error 
		  :fb-error-code 30 
		  :fb-error-text "Unable to allocate statement"
		  :fbclient-msg (get-status-vector-msg status-vector*))
	(cffi-sys:foreign-free status-vector*)))
    (isc-dsql-prepare status-vector*
		      (transaction-handle* (fb-tr fb-stmt))
		      (statement-handle* fb-stmt)
		      0 
		      (cffi:foreign-string-alloc (request-str fb-stmt)) 
		      0 
		      (cffi:null-pointer))
    (when (status-vector-error-p status-vector*)
      (unwind-protect
	   (error 'fb-error 
		  :fb-error-code 31 
		  :fb-error-text (format nil "Unable to prepare statement: ~a"
					 (request-str fb-stmt))
		  :fbclient-msg (get-status-vector-msg status-vector*))
	(cffi-sys:foreign-free status-vector*)))
    (setf (st-type fb-stmt) (get-sql-type (statement-handle* fb-stmt)))
    (setf (xsqlda-output* fb-stmt) (make-xsqlda 10))
    (isc-dsql-describe status-vector* 
		       (statement-handle* fb-stmt)
		       1 
		       (xsqlda-output* fb-stmt))
    (when (status-vector-error-p status-vector*)
      (unwind-protect
	   (error 'fb-error 
		  :fb-error-code 32 
		  :fb-error-text "Error in isc-dsql-describe"
		  :fbclient-msg (get-status-vector-msg status-vector*))
	 (cffi-sys:foreign-free status-vector*)))
     (setf (xsqlda-output* fb-stmt) (remake-xsqlda (xsqlda-output* fb-stmt)))
     (alloc-vars-data (xsqlda-output* fb-stmt))
     (isc-dsql-execute status-vector*
		       (transaction-handle* (fb-tr fb-stmt))
		       (statement-handle* fb-stmt)
		       1 
		       (cffi:make-pointer 0))
     (when (status-vector-error-p status-vector*)
       (unwind-protect
	    (error 'fb-error 
		   :fb-error-code 33 
		   :fb-error-text "Unable to execute statement"
		   :fbclient-msg (get-status-vector-msg status-vector*))
	 (cffi-sys:foreign-free status-vector*)))
     (cffi-sys:foreign-free status-vector*)))
;-----------------------------------------------------------------------------------
(defmethod initialize-instance :after ((stmt fb-statement) &key (no-auto-execute Nil) (no-auto-prepare Nil) (no-auto-allocate Nil))
  (when (null no-auto-allocate) 
    (fb-allocate-statement stmt)
    (when (null no-auto-prepare) 
      (fb-prepare-statement stmt)
      (when (null no-auto-execute)
	(fb-execute-statement stmt)))))
;-----------------------------------------------------------------------------------
(defun fb-statement-free (stmt)
  "Method to free statement."
  (let ((status-vector* (make-status-vector)))
    (isc-dsql-free-statement status-vector* (statement-handle* stmt) 1)
    (unwind-protect
	 (when (status-vector-error-p status-vector*)
	   (error 'fb-error 
		  :fb-error-code 35 
		  :fb-error-text "Unable to free statement"
		  :fbclient-msg (get-status-vector-msg status-vector*)))
      (cffi-sys:foreign-free status-vector*))))
;-----------------------------------------------------------------------------------
(defmethod fb-statement-fetch ((stmt fb-statement))
  "Method to fetch results from executed statement."
  (if (eq (fb-get-sql-type stmt) 'select)
      (let ((status-vector* (make-status-vector)))
	(let ((fetch-res (isc-dsql-fetch status-vector* (statement-handle* stmt) 1 (xsqlda-output* stmt))))
	  (unwind-protect
	       (when (status-vector-error-p status-vector*)
		 (error 'fb-error 
			:fb-error-code 36 
			:fb-error-text "Unable to fetch statement"
			:fbclient-msg (get-status-vector-msg status-vector*)))
	    (cffi-sys:foreign-free status-vector*))
	  (when(= fetch-res 0) T)))
      (error 'fb-error 
		  :fb-error-code 36 
		  :fb-error-text "Unable to fetch statement. Statement type is not SELECT."
		  :fbclient-msg "")))
;-----------------------------------------------------------------------------------
(defun fb-statement-get-var-val (stmt index)
  "A method for obtaining the values of result variables. Used after Fetch."
  (get-var-val (xsqlda-output* stmt) index))
;-----------------------------------------------------------------------------------
(defun fb-statement-get-vars-vals-list (stmt)
  "A method for obtaining the list of values ​​of result variables. Used after Fetch."
  (get-vars-vals-list (xsqlda-output* stmt)))
;-----------------------------------------------------------------------------------
(defun fb-statement-get-var-val+name (stmt index)
  "A method for obtaining the values and names of result variables. Used after Fetch."
  (get-var-val+name (xsqlda-output* stmt) index))
;-----------------------------------------------------------------------------------
(defun fb-statement-get-vars-vals+names-list (stmt)
  "A method for obtaining the list of values and names of result variables. Used after Fetch."
  (get-vars-vals+names-list (xsqlda-output* stmt)))
;-----------------------------------------------------------------------------------
(defun fb-statement-get-vars-names-list (stmt)
  "A method for obtaining names of result variables. Used after Fetch."
  (get-vars-names (xsqlda-output* stmt)))
;===================================================================================
;; 'FB-WIDTH-..' and 'FB-LOOP-..' macroses
;-----------------------------------------------------------------------------------
(defmacro fb-with-transaction ((fb-db transaction-name) &body body)
  "Macro to create, automatic start and commit transactions."
  `(let ((,transaction-name (make-instance 'fb-transaction :fb-db ,fb-db)))
     (unwind-protect 
	 (progn ,@body)
     (fb-commit-transaction ,transaction-name))))
;-----------------------------------------------------------------------------------
(defmacro fb-with-statement ((fb-tr statement-name request-str) &body body)
  "Macro to create, automatic allocate and free statements."
  `(let ((,statement-name (make-instance 'fb-statement :fb-tr ,fb-tr :request-str ,request-str)))
     (unwind-protect 
	 (progn ,@body)
     (fb-statement-free ,statement-name))))
;-----------------------------------------------------------------------------------
(defmacro fb-with-statement-db ((fb-db statement-name request-str) &body body)
  "Macro to create, automatic allocate and free statements. 
   (transaction will be created, started and commited automatically)"
  (let ((tr-name (gensym)))
   `(fb-with-transaction (,fb-db ,tr-name)
			 (fb-with-statement (,tr-name ,statement-name ,request-str)
					    ,@body))))
;-----------------------------------------------------------------------------------
(defmacro fb-loop-statement-fetch ((fb-stmt) &body body)
  "Macro to loop reading and processing the query results"
  `(loop while (fb-statement-fetch ,fb-stmt) 
      do (progn ,@body)))
;-----------------------------------------------------------------------------------
(defmacro fb-loop-query-fetch ((fb-db request-str varlist) &body body)
  "Macro to loop reading and processing the query results by DB.
   (transaction will be created, started and commited automatically)"
  (let ((st-name (gensym)))
   `(fb-with-statement-db (,fb-db ,st-name ,request-str)
      (loop while (fb-statement-fetch ,st-name) 
	 do (let ,(loop for i from 0 to (- (length varlist) 1) 
		     collect `(,(nth i varlist) 
				(fb-statement-get-var-val ,st-name ,i)))
	      (progn ,@body))))))
;-----------------------------------------------------------------------------------
;===================================================================================
;; QUERY functions 
;; (automatic 'start' and 'commit' transaction)
;; (automatic 'allocate' and 'free' statement)
;-----------------------------------------------------------------------------------
(defun fb-noresult-query (fb-db request-str)
  "A method for performing queries that do not require answers.(insert,delete,update, etc.) 
(transaction will be created, started and commited automatically)"
  (fb-with-statement-db (fb-db st-tmp-name request-str) nil))
;-----------------------------------------------------------------------------------
(defun fb-query-fetch-all (fb-db request-str)
  "The method, which executes the query and returns all its results in a list. 
(transaction will be created, started and commited automatically)"
  (fb-with-statement-db (fb-db st-tmp-name request-str)
			(loop while (fb-statement-fetch st-tmp-name)
			     collect (fb-statement-get-vars-vals-list st-tmp-name))))
;-----------------------------------------------------------------------------------
(defun fb-query-fetch-all+names (fb-db request-str)
  "The method, which executes the query and returns all its results(+names) in a list. 
(transaction will be created, started and commited automatically)"
  (fb-with-statement-db (fb-db st-tmp-name request-str)
    (loop while (fb-statement-fetch st-tmp-name)
       collect (fb-statement-get-vars-vals+names-list st-tmp-name))))
;-----------------------------------------------------------------------------------
(defun fb-query-fetch-all+names-header (fb-db request-str)
  "The method, which executes the query and returns all its results(+names header) in a list. 
(transaction will be created, started and commited automatically)"
  (fb-with-statement-db (fb-db st-tmp-name request-str)
    (append (list (fb-statement-get-vars-names-list st-tmp-name))
	    (loop while (fb-statement-fetch st-tmp-name)
	       collect (fb-statement-get-vars-vals-list st-tmp-name)))))
;-----------------------------------------------------------------------------------
;===================================================================================
