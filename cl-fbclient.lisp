;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; cl-fbclient.lisp
;;;; CONDITIONS and CLASSES for "cl-fbclient"
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(in-package #:cl-fbclient)
;===================================================================================
(defgeneric fb-verbalize-error (err))
(defgeneric fb-connect (fb-db) 
  (:documentation "Method to connect to the database."))
(defgeneric fb-disconnect (db)
  (:documentation "Method to disconnect from the database."))
(defgeneric fb-start-transaction (transaction)
  (:documentation "Method to start transaction."))
(defgeneric fb-commit-transaction (transaction)
  (:documentation "Method to commit transaction."))
(defgeneric fb-rollback-transaction (transaction)
  (:documentation "Method to rollback transaction."))
(defgeneric fb-prepare-and-execute-statement (statement)
  (:documentation "Method to prepare and execute statement."))
(defgeneric fb-statement-free (statement)
  (:documentation "Method to free statement."))
(defgeneric fb-statement-fetch (statement)
  (:documentation "Method to fetch results from executed statement."))
(defgeneric fb-statement-get-var-val (statement index)
  (:documentation "A method for obtaining the values of result variables. Used after Fetch."))
(defgeneric fb-statement-get-vars-vals-list (statement)
  (:documentation "A method for obtaining the list of values ​​of result variables. Used after Fetch."))
(defgeneric fb-noresult-query (fb-db request-str)
  (:documentation "A method for performing queries that do not require answers.(insert,delete,update, etc.) (transaction will be created, started and commited automatically)"))
(defgeneric fb-query-fetch-all (fb-db request-str)
  (:documentation "The method, which executes the query and returns all its results in a list. (transaction will be created, started and commited automatically)"))
;===================================================================================
;; FB-ERROR
;-----------------------------------------------------------------------------------
(define-condition fb-error (error)
  ((fb-error-code :initarg :fb-error-code :reader fb-error-code)
   (fb-error-text :initarg :fb-error-text :reader fb-error-text)
   (fbclient-msg :initarg :fbclient-msg :reader fbclient-msg)))
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
	     :initform "masterkey")))
;-----------------------------------------------------------------------------------
(defmethod fb-connect ((fb-db fb-database))
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
(defmethod fb-disconnect ((db fb-database))
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
   (transaction-handle* :accessor transaction-handle*)))
;-----------------------------------------------------------------------------------
(defmethod fb-start-transaction ((tr fb-transaction))
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
(defmethod fb-commit-transaction ((tr fb-transaction))
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
(defmethod fb-rollback-transaction ((tr fb-transaction))
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
	 :initform Nil)))
;-----------------------------------------------------------------------------------
(defmethod fb-prepare-and-execute-statement ((fb-stmt fb-statement))
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
		      1 
		      (cffi-sys:make-pointer 0))
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
     ;; (when (eq (fb-get-sql-type fb-stmt) 'select)
     ;;   (isc-dsql-set-cursor-name status-vector* 
     ;; 				 (statement-handle* fb-stmt)
     ;; 				 (cffi:foreign-string-alloc "dyn_cursor") 0)
     ;;   (when (status-vector-error-p status-vector*)
     ;; 	 (unwind-protect
     ;; 	      (error 'fb-error 
     ;; 		     :fb-error-code 34 
     ;; 		     :fb-error-text "Unable to make cursor"
     ;; 		     :fbclient-msg (get-status-vector-msg status-vector*))
     ;; 	   (cffi-sys:foreign-free status-vector*))))
     (cffi-sys:foreign-free status-vector*)))
;-----------------------------------------------------------------------------------
(defmethod initialize-instance :after ((stmt fb-statement) &key (no-auto-prepare-and-execute Nil))
  (when (null no-auto-prepare-and-execute) (fb-prepare-and-execute-statement stmt)))
;-----------------------------------------------------------------------------------
(defmethod fb-statement-free ((stmt fb-statement))
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
(defmethod fb-statement-get-var-val ((stmt fb-statement) index)
  (get-var-val (xsqlda-output* stmt) index))
;-----------------------------------------------------------------------------------
(defmethod fb-statement-get-vars-vals-list ((stmt fb-statement))
  (get-vars-vals-list (xsqlda-output* stmt)))
;-----------------------------------------------------------------------------------
;===================================================================================
;; 'FB-WIDTH-..' and 'FB-LOOP-..' macroses
;-----------------------------------------------------------------------------------
(defmacro fb-with-transaction ((fb-db transaction-name) &rest body)
  `(let ((,transaction-name (make-instance 'fb-transaction :fb-db ,fb-db)))
     (unwind-protect 
	 (progn ,@body)
     (fb-commit-transaction ,transaction-name))))
;-----------------------------------------------------------------------------------
(defmacro fb-with-statement ((fb-tr statement-name request-str) &rest body)
  `(let ((,statement-name (make-instance 'fb-statement :fb-tr ,fb-tr :request-str ,request-str)))
     (unwind-protect 
	 (progn ,@body)
     (fb-statement-free ,statement-name))))
;-----------------------------------------------------------------------------------
(defmacro fb-with-statement-db ((fb-db statement-name request-str) &rest body)
  (let ((tr-name (gensym)))
   `(fb-with-transaction (,fb-db ,tr-name)
			 (fb-with-statement (,tr-name ,statement-name ,request-str)
					    ,@body))))
;-----------------------------------------------------------------------------------
(defmacro fb-loop-statement-fetch ((fb-stmt) &rest body)
  `(loop while (fb-statement-fetch ,fb-stmt) 
      do (progn ,@body)))
;-----------------------------------------------------------------------------------
(defmacro fb-loop-query-fetch ((fb-db request-str varlist) &rest body)
  (let ((tr-name (gensym))
	(st-name (gensym)))
   `(fb-with-transaction 
     (,fb-db ,tr-name)
     (fb-with-statement 
      (,tr-name ,st-name ,request-str)
      (loop while (fb-statement-fetch ,st-name) 
	 do (let ,(loop for i from 0 to (- (length varlist) 1) 
		     collect `(,(nth i varlist) 
				(fb-statement-get-var-val ,st-name ,i)))
	      (progn ,@body)))))))
;-----------------------------------------------------------------------------------
;===================================================================================
;; QUERY functions 
;; (automatic 'start' and 'commit' transaction)
;; (automatic 'allocate' and 'free' statement)
;-----------------------------------------------------------------------------------
(defmethod fb-noresult-query ((fb-db fb-database) request-str)
  (fb-with-statement-db (fb-db st-tmp-name request-str) nil))
;-----------------------------------------------------------------------------------
(defmethod fb-query-fetch-all ((fb-db fb-database) request-str)
  (fb-with-statement-db (fb-db st-tmp-name request-str)
			(loop while (fb-statement-fetch st-tmp-name)
			     collect (fb-statement-get-vars-vals-list st-tmp-name))))
;-----------------------------------------------------------------------------------
;===================================================================================
