;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; cl-fbclient.lisp
;;;; MACROS for "cl-fbclient"
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(in-package #:cl-fbclient)
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
;; :vars-names <optional (list ...)> - Add variable name or name from <list> to value like :name ~val~
;; :one-record - Read only one record.
;-----------------------------------------------------------------------------------
(defmacro fb-query (request-str &rest kpar)
  (let ((tmp-stmt (gensym "STMT"))) 
    (unless (evenp (length kpar)) (setf kpar (append kpar '(Nil))))
    (append 
     (cond ((getf kpar :db)
	    `(fb-with-statement-db (,(getf kpar :db) ,tmp-stmt ,request-str)))
	   ((getf kpar :tr)
	    `(fb-with-statement (,(getf kpar :tr) ,tmp-stmt ,request-str)))
	   (T `(fb-with-statement-db (*database-toplevel* ,tmp-stmt ,request-str))))
     `((when (eq (fb-get-sql-type ,tmp-stmt) 'select)
	   ,(let ((bdy 
	     (let*((mmbr-vars-names  (member :vars-names kpar))
                   (func (if mmbr-vars-names
                              (if (keywordp (second mmbr-vars-names))
				  `(fb-statement-get-vars-vals+names-list ,tmp-stmt)
                                  `(fb-statement-get-vars-vals+names-list 
                                    ,tmp-stmt 
                                    ,(second mmbr-vars-names)))
                              `(fb-statement-get-vars-vals-list ,tmp-stmt))))
                (if (member :one-record kpar)
                    `(when (fb-statement-fetch ,tmp-stmt)
                       ,func)
                    `(loop while (fb-statement-fetch ,tmp-stmt)
		       collect ,func)))))
		 (if (member :header-names kpar)
		     `(cons (fb-statement-get-vars-names-list ,tmp-stmt) 
			    ,(if (member :one-record kpar) `(list ,bdy) bdy))
		     bdy)))))))
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
;; TOPLEVEL
;-----------------------------------------------------------------------------------
(defparameter *database-toplevel* nil)
;-----------------------------------------------------------------------------------
(defun fb-connect-toplevel (&key (host "localhost") path 
			      (user-name "SYSDBA") (password "masterkey"))
  (setf *database-toplevel* 
	(make-instance 'FB-DATABASE 
		       :host host
		       :path path
		       :user-name user-name
		       :password password)))
;-----------------------------------------------------------------------------------
(defun fb-disconnect-toplevel ()
  (fb-disconnect *database-toplevel*))
;-----------------------------------------------------------------------------------
(defmacro fb-with-toplevel-connection ((&rest params) &body body)
  `(fb-with-database (*database-toplevel* ,@params)
		     ,@body))
;-----------------------------------------------------------------------------------
;===================================================================================
;-----------------------------------------------------------------------------------
(defun fb-connected-p (&optional (fb-db *database-toplevel*))
  ""
  (if (db-handle* fb-db) T Nil))
;-----------------------------------------------------------------------------------
