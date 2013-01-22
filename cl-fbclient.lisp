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
;===================================================================================
;; PARAMETERS
;-----------------------------------------------------------------------------------
;; *timestamp-alist-converter* - defined in "cl-fbclient-functions.lisp"
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
;-----------------------------------------------------------------------------------
(defmethod print-object ((err fb-error) stream)
  (format stream (fb-verbalize-error err)))
;-----------------------------------------------------------------------------------
(defmacro with-status-vector (status-vector* &body body)
  `(let ((,status-vector* (make-status-vector))) 
       (unwind-protect
	    ,@body
	 (cffi-sys:foreign-free ,status-vector*))))
;-----------------------------------------------------------------------------------
(defmacro process-status-vector (status-vector* err-code err-text)
  `(when (status-vector-error-p ,status-vector*)
     (error 'fb-error 
	    :fb-error-code ,err-code
	    :fb-error-text ,err-text
	    :fbclient-msg (get-status-vector-msg ,status-vector*))))
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
  (with-status-vector status-vector*
    (let ((host+path (concatenate 'string (host fb-db) ":" (path fb-db))))
      (connect-to-db (db-handle* fb-db) status-vector* host+path (user-name fb-db) (password fb-db))
      (process-status-vector status-vector* 10 (format nil "Unable to connect ('~a')" host+path)))))
;-----------------------------------------------------------------------------------
(defmethod initialize-instance :after ((db fb-database) &key (no-auto-connect Nil))
  (progn (setf (db-handle* db) (make-db-handler))
	 (when (null no-auto-connect) (fb-connect db))))
;-----------------------------------------------------------------------------------
(defun fb-disconnect (db)
  "Method to disconnect from the database."
  (with-status-vector status-vector*
    (isc-detach-database status-vector* (db-handle* db))
    (process-status-vector status-vector* 11 "Error when disconnecting from DB")))
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
  (with-status-vector status-vector*
    (start-transaction (db-handle* (fb-db tr)) (transaction-handle* tr) status-vector*)
    (process-status-vector status-vector* 20 "Unable to start transaction")))
;-----------------------------------------------------------------------------------
(defmethod initialize-instance :after ((tr fb-transaction) &key (no-auto-start Nil))
  (progn (setf (transaction-handle* tr) (make-tr-handler))
	 (when (null no-auto-start) (fb-start-transaction tr))))
;-----------------------------------------------------------------------------------
(defun fb-commit-transaction (tr)
  "Method to commit transaction."
  (with-status-vector status-vector*
    (isc-commit-transaction status-vector* (transaction-handle* tr))
    (process-status-vector status-vector* 21 "Unable to commit transaction")))
;-----------------------------------------------------------------------------------
(defun fb-rollback-transaction (tr)
  "Method to rollback transaction."
  (with-status-vector status-vector*
    (isc-rollback-transaction status-vector* (transaction-handle* tr))
    (process-status-vector status-vector* 22 "Unable to rollback transaction")))
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
  (with-status-vector status-vector*
    (isc-dsql-allocate-statement status-vector*
				 (db-handle* (fb-db (fb-tr fb-stmt)))
				 (statement-handle* fb-stmt))
    (process-status-vector status-vector* 30 "Unable to allocate statement")))
;-----------------------------------------------------------------------------------
(defun fb-prepare-statement (fb-stmt)
  "Method to prepare statement."
  (with-status-vector status-vector*
    (isc-dsql-prepare status-vector*
		      (transaction-handle* (fb-tr fb-stmt))
		      (statement-handle* fb-stmt)
		      0 
		      (cffi:foreign-string-alloc (request-str fb-stmt)) 
		      0 
		      (cffi:null-pointer))
    (process-status-vector status-vector* 
			   31 (format nil "Unable to prepare statement: ~a"
						     (request-str fb-stmt)))
    (setf (st-type fb-stmt) (get-sql-type (statement-handle* fb-stmt)))
    (setf (xsqlda-output* fb-stmt) (make-xsqlda 10))
    (isc-dsql-describe status-vector* 
		       (statement-handle* fb-stmt)
		       1 
		       (xsqlda-output* fb-stmt))
    (process-status-vector status-vector* 
			   32 "Error in isc-dsql-describe")
    (when (need-remake-xsqlda (xsqlda-output* fb-stmt))
      (setf (xsqlda-output* fb-stmt) (remake-xsqlda (xsqlda-output* fb-stmt)))
      (isc-dsql-describe status-vector* 
			 (statement-handle* fb-stmt)
			 1 
			 (xsqlda-output* fb-stmt))
      (process-status-vector status-vector* 
			     32 "Error in isc-dsql-describe"))
    (alloc-vars-data (xsqlda-output* fb-stmt))))
;-----------------------------------------------------------------------------------
(defun fb-execute-statement (fb-stmt)
  "Method to execute statement."
  (with-status-vector status-vector*
     (isc-dsql-execute status-vector*
		       (transaction-handle* (fb-tr fb-stmt))
		       (statement-handle* fb-stmt)
		       1 
		       (cffi:make-pointer 0))
     (process-status-vector status-vector* 33 "Unable to execute statement")))
;-----------------------------------------------------------------------------------
(defmethod initialize-instance :after ((stmt fb-statement) 
				       &key (no-auto-execute Nil) 
				       (no-auto-prepare Nil) (no-auto-allocate Nil))
  (unless no-auto-allocate 
    (fb-allocate-statement stmt)
    (unless no-auto-prepare
      (fb-prepare-statement stmt)
      (unless no-auto-execute
	(fb-execute-statement stmt)))))
;-----------------------------------------------------------------------------------
(defun fb-statement-free (stmt)
  "Method to free statement."
  (with-status-vector status-vector*
    (isc-dsql-free-statement status-vector* (statement-handle* stmt) 1)
    (process-status-vector status-vector* 35 "Unable to free statement")))
;-----------------------------------------------------------------------------------
(defmethod fb-statement-fetch ((stmt fb-statement))
  "Method to fetch results from executed statement."
  (if (eq (fb-get-sql-type stmt) 'select)
      (with-status-vector status-vector*
      	(let ((fetch-res (isc-dsql-fetch status-vector* (statement-handle* stmt) 1 (xsqlda-output* stmt))))
	  (process-status-vector status-vector* 36 "Unable to fetch statement")
	  (when (= fetch-res 0) T)))
      (error 'fb-error 
		  :fb-error-code 36 
		  :fb-error-text "Unable to fetch statement. Statement type is not SELECT."
		  :fbclient-msg "")))
;-----------------------------------------------------------------------------------
(defun fb-statement-get-var-val (stmt index)
  "A method for obtaining the values of result variables. Used after Fetch."
  (get-var-val (xsqlda-output* stmt) index))
;-----------------------------------------------------------------------------------
(defun fb-statement-get-vars-vals-list (stmt &rest rst)
  "A method for obtaining the list of values ​​of result variables. Used after Fetch."
  (get-vars-vals-list (xsqlda-output* stmt)))
;-----------------------------------------------------------------------------------
(defun fb-statement-get-var-val+name (stmt index)
  "A method for obtaining the values and names of result variables. Used after Fetch."
  (get-var-val+name (xsqlda-output* stmt) index))
;-----------------------------------------------------------------------------------
(defun fb-statement-get-vars-vals+names-list (stmt &optional (names Nil))
  "A method for obtaining the list of values and names of result variables. Used after Fetch."
  (get-vars-vals+names-list (xsqlda-output* stmt) names))
;-----------------------------------------------------------------------------------
(defun fb-statement-get-vars-names-list (stmt)
  "A method for obtaining names of result variables. Used after Fetch."
  (get-vars-names (xsqlda-output* stmt)))
;===================================================================================
;; 'FB-WIDTH-..' and 'FB-LOOP-..' macroses
;-----------------------------------------------------------------------------------
(defmacro fb-with-database ((database-name &rest params) &body body)
  "Macro to automatic connect and disconnect database."
  `(let ((,database-name (make-instance 'fb-database ,@params)))
     (unwind-protect 
	 (progn ,@body)
     (fb-disconnect ,database-name))))
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
;;;  FB-QUERY macro
;; working with transactions or databases(transaction will be created, started and commited automatically)
;; parameters:
;; ':db' or ':tr' fb-database or fb-transaction
;; :header-names - Add header(which contains names of variables) to values list 
;; :vars-names - Add variable name to value like :name ~val~
;; :one-record - Read only one record.
;-----------------------------------------------------------------------------------
(defmacro fb-query (request-str &rest kpar)
  (progn 
    (unless (evenp (length kpar)) (setf kpar (append kpar '(Nil))))
    (append 
     (cond ((getf kpar :db)
	    `(fb-with-statement-db (,(getf kpar :db) tmp-stmt ,request-str)))
	   ((getf kpar :tr)
	    `(fb-with-statement (,(getf kpar :tr) tmp-stmt ,request-str)))
	   (T 'ERR))
     `((when (eq (fb-get-sql-type tmp-stmt) 'select)
	   ,(append 
	     (if (member :header-names kpar)
		  '(list (fb-statement-get-vars-names-list tmp-stmt))
		  'Nil)
	     (let*((mmbr-vars-names  (member :vars-names kpar))
                   (func (if mmbr-vars-names
                              (if (listp (second mmbr-vars-names))
                                  `(fb-statement-get-vars-vals+names-list 
                                    tmp-stmt 
                                    ,(second mmbr-vars-names))
                                  '(fb-statement-get-vars-vals+names-list tmp-stmt))
                              '(fb-statement-get-vars-vals-list tmp-stmt))))
                (if (member :one-record kpar)
                    `(when (fb-statement-fetch tmp-stmt)
                       ,func)
                    `(loop while (fb-statement-fetch tmp-stmt)
		       collect ,func)))))))))
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
