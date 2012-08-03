;;;; cl-fbclient.lisp

(in-package #:cl-fbclient)

;;; "cl-fbclient" goes here. Hacks and glory await!

;===================================================================================
(cffi:define-foreign-library
	     fbclient
  (:unix (:or "libfbclient.so.2" "libfbclient.so"))
  (t (:default "libfbclient")))

(cffi:use-foreign-library fbclient)
;===================================================================================
(cffi:defctype isc_db_handle :unsigned-int)
(cffi:defctype isc_status :long)
(cffi:defctype isc_tr_handle :unsigned-int)
(cffi:defctype isc_stmt_handle :unsigned-int)
;===================================================================================
(cffi:defcstruct XSQLVAR 
  (sqltype :short)
  (sqlscale :short)
  (sqlsubtype :short)
  (sqllen :short)
  (sqldata :pointer)
  (sqlind :pointer)
  (sqlname_length :short)
  (sqlname :char :count 32)
  (relname_length :short)
  (relname :char :count 32)
  (ownname_length :short)
  (ownname :char :count 32)
  (aliasname_length :short)
  (aliasname :char :count 32))
;-----------------------------------------------------------------------------------
(cffi:defcstruct XSQLDA
  (version :short)
  (sqldaid :char :count 8)
  (sqldabc :int)
  (sqln :short)
  (sqld :short)
  (sqlvar XSQLVAR :count 1))
;===================================================================================
(cffi:defcfun "isc_attach_database" :long 
  (isc_status_vect :pointer)
  (isc_path_len :short)
  (isc_path :pointer)
  (isc_db_h :pointer)
  (isc_dpb_buf_len :short)
  (isc_dpb_buf :pointer))
;-----------------------------------------------------------------------------------
(cffi:defcfun "isc_detach_database" :long
  (isc_status_vect :pointer)
  (isc_db_h :pointer))
;-----------------------------------------------------------------------------------
(cffi:defcfun "isc_print_status" :char 
  (isc_status_vect :pointer))
;-----------------------------------------------------------------------------------
(cffi:defcfun "isc_interprete" :long 
  (isc_msg :pointer)
  (isc_status_vect :pointer))
;-----------------------------------------------------------------------------------
(cffi:defcfun "isc_sqlcode" :short 
  (isc_status_vect :pointer))
;-----------------------------------------------------------------------------------
(cffi:defcfun "isc_sql_interprete" :long 
  (isc_sql_code :short)
  (isc_msg :pointer)
  (isc_msg_size :short))
;-----------------------------------------------------------------------------------
(cffi:defcfun "isc_start_transaction" :long
  (isc_status_vect :pointer)
  (isc_tr_h :pointer)
  (isc_db_count :short)
  (isc_db_h :pointer)
  (isc_tpb_length :unsigned-short)
  (isc_tpb :pointer))
;-----------------------------------------------------------------------------------
(cffi:defcfun "isc_commit_transaction" :long
  (isc_status_vect :pointer)
  (isc_tr_h :pointer))
;-----------------------------------------------------------------------------------
(cffi:defcfun "isc_rollback_transaction" :long
  (isc_status_vect :pointer)
  (isc_tr_h :pointer))
;-----------------------------------------------------------------------------------
(cffi:defcfun "isc_dsql_allocate_statement" :long
  (isc_status_vect :pointer)
  (isc_db_h :pointer)
  (isc_stmp_h :pointer))
;-----------------------------------------------------------------------------------
(cffi:defcfun "isc_dsql_prepare" :long
  (isc_status_vect :pointer)
  (isc_tr_h :pointer)
  (isc_stmp_h :pointer)
  (isc_zero :unsigned-short)
  (isc_dsql_str :pointer)
  (isc_one :unsigned-short)
  (isc_xsqlda :pointer))
;-----------------------------------------------------------------------------------
(cffi:defcfun "isc_dsql_describe" :long
  (isc_status_vect :pointer)
  (isc_stmt_h :pointer)
  (isc_one :unsigned-short)
  (isc_xsqlda :pointer))
;-----------------------------------------------------------------------------------
(cffi:defcfun "isc_dsql_set_cursor_name" :long
  (isc_status_vect :pointer)
  (isc_stmt_h :pointer)
  (isc_name :pointer)
  (isc_null :unsigned-short))
;-----------------------------------------------------------------------------------
(cffi:defcfun "isc_dsql_execute" :long
  (isc_status_vect :pointer)
  (isc_tr_h :pointer)
  (isc_stmp_h :pointer)
  (isc_one :unsigned-short)
  (isc_xsqlda :pointer))
;-----------------------------------------------------------------------------------
(cffi:defcfun "isc_dsql_fetch" :long
  (isc_status_vect :pointer)
  (isc_stmp_h :pointer)
  (isc_one :unsigned-short)
  (isc_xsqlda :pointer))
;-----------------------------------------------------------------------------------
(cffi:defcfun "isc_dsql_sql_info" :long
  (isc_status_vect :pointer)
  (isc_stmp_h :pointer)
  (isc_one :short)
  (isc_req :pointer)
  (isc_size :short)
  (isc_res :pointer))
;-----------------------------------------------------------------------------------
(cffi:defcfun "isc_dsql_free_statement" :long
  (isc_status_vect :pointer)
  (isc_stmt_h :pointer)
  (isc_code_one :unsigned-short))
;-----------------------------------------------------------------------------------
;===================================================================================
(defun make-db-handler ()
  (cffi:foreign-alloc 'isc_db_handle :initial-element 0))
;-----------------------------------------------------------------------------------
(defun make-tr-handler ()
  (cffi:foreign-alloc 'isc_tr_handle :initial-element 0))
;-----------------------------------------------------------------------------------
(defun make-stmt-handler ()
  (cffi:foreign-alloc 'isc_stmt_handle :initial-element 0))
;-----------------------------------------------------------------------------------
(defun make-status-vector ()
  (cffi:foreign-alloc 'isc_status :count 20))
;-----------------------------------------------------------------------------------
(defmacro setf-dpb-buff (dpb-buff-var dpb-buff-size-var db-user-name db-password)
  `(let ((u_name-len (length ,db-user-name))
	 (u_pass-len (length ,db-password)))
     (when ,dpb-buff-var (cffi-sys:foreign-free ,dpb-buff-var))
     (setf ,dpb-buff-size-var (+ 5 u_name-len u_pass-len))
     (setf ,dpb-buff-var (cffi:foreign-alloc :char 
					     :count ,dpb-buff-size-var 
					     :initial-element 0))
     (setf (cffi:mem-aref ,dpb-buff-var :char 0) 1)
     (setf (cffi:mem-aref ,dpb-buff-var :char 1) 28)
     (setf (cffi:mem-aref ,dpb-buff-var :char 2) u_name-len)
     (loop for i from 0 to (- u_name-len 1) 
	do (setf (cffi:mem-aref ,dpb-buff-var :char (+ i 3)) 
		 (char-code (char ,db-user-name i))))
     (setf (cffi:mem-aref ,dpb-buff-var :char (+ 3 u_name-len)) 29)
     (setf (cffi:mem-aref ,dpb-buff-var :char (+ 4 u_name-len)) u_pass-len)
     (loop for i from 0 to (- u_pass-len 1) 
	do (setf (cffi:mem-aref ,dpb-buff-var :char (+ i 5 u_name-len)) 
		 (char-code (char ,db-password i))))))
;-----------------------------------------------------------------------------------
(defun calc-dpb-size (username password)
  (+ 5 (length username) (length password)))
;-----------------------------------------------------------------------------------
(defun make-dpb (username password)
  (let ((dpb nil) (dpb-len nil))
    (setf-dpb-buff dpb dpb-len username password)
    dpb))
;-----------------------------------------------------------------------------------
(defun connect-to-db (db-handle-pointer status-vector-pointer host+path login pass)
  (isc-attach-database status-vector-pointer (length host+path) 
		       (cffi:foreign-string-alloc host+path)
		       db-handle-pointer
		       (calc-dpb-size login pass)
		       (make-dpb login pass)))
;-----------------------------------------------------------------------------------
(defun start-transaction (db-handle-pointer transaction-pointer status-vector-pointer)
  (isc-start-transaction status-vector-pointer transaction-pointer 1 db-handle-pointer 0 (cffi-sys:make-pointer 0)))
;-----------------------------------------------------------------------------------
(defun XSQLDA-length (n)
  (+ (cffi:foreign-type-size 'XSQLDA) (* (- n 1) (cffi:foreign-type-size 'XSQLVAR)))) 
;-----------------------------------------------------------------------------------
(defun make-xsqlda (n)
  (let ((new-xsqlda (cffi:foreign-alloc :char :count (XSQLDA-length n) :initial-element 0)))
     (setf (cffi:foreign-slot-value new-xsqlda 'xsqlda 'version) 1)
     (setf (cffi:foreign-slot-value new-xsqlda 'xsqlda 'sqln) n)
     new-xsqlda))
;  (cffi:foreign-alloc :char :count (XSQLDA-length n) :initial-element 0))
;-----------------------------------------------------------------------------------
(defun remake-xsqlda (tmp-xsqlda)
  (cond ((> (cffi:foreign-slot-value tmp-xsqlda 'xsqlda 'sqld) 
	    (cffi:foreign-slot-value tmp-xsqlda 'xsqlda 'sqln))
	(let ((new-xsqlda (make-xsqlda 
			   (cffi:foreign-slot-value tmp-xsqlda 'xsqlda 'sqld))))
	  (cffi-sys:foreign-free tmp-xsqlda)
	  ;(format t "XSQLDA updated")
	  new-xsqlda))
	(T tmp-xsqlda)))
;-----------------------------------------------------------------------------------
(defun get-var-type-by-fbtype-num (type-num)
 ; (prog1  
      (cond 
	   ((= type-num 496) ':long)
	   ((= type-num 500) ':short)
	   ((= type-num 482) ':float)
	   ((= type-num 480) ':double)
	   ((= type-num 452) ':text)
	   ((= type-num 448) ':varying)
	   ;...
	   ;TODO: other types
	   )
  ;  (print type-num)
	 );)
;-----------------------------------------------------------------------------------
(defun get-var-type (xsqlda* index)
  (let ((tp (cffi:foreign-slot-value 
		  (cffi:mem-aref 
		   (cffi:foreign-slot-value xsqlda* 'xsqlda 'sqlvar) 'xsqlvar index) 
		  'xsqlvar 'sqltype))
	(can-nil T))
    (cond ((oddp tp) (decf tp))
	  (T (setf can-nil nil)))
    (values-list (list (get-var-type-by-fbtype-num tp) can-nil))))
;-----------------------------------------------------------------------------------
(defun get-var-sqlln (xsqlda* index)
  (cffi:foreign-slot-value 
   (cffi:mem-aref 
    (cffi:foreign-slot-value xsqlda* 'xsqlda 'sqlvar) 
    'xsqlvar index) 'xsqlvar 'sqllen))
;-----------------------------------------------------------------------------------
(defun alloc-var-data-by-type (sqlda index type initial-elt &key (count 1))
  (setf (cffi:foreign-slot-value 
	 (cffi:mem-aref 
	  (cffi:foreign-slot-value sqlda 'xsqlda 'sqlvar) 
	  'xsqlvar index) 'xsqlvar 'sqldata) 
	(cffi:foreign-alloc type :initial-element initial-elt :count count)))
;-----------------------------------------------------------------------------------
(defun alloc-var-data-text (sqlda index)
  (alloc-var-data-by-type sqlda index :char 0 
			  :count (+ 1 (get-var-sqlln sqlda index))))
;-----------------------------------------------------------------------------------
(defun alloc-var-data-varying (sqlda index)
  (alloc-var-data-by-type sqlda index :char 0 
			  :count (+ 3 (get-var-sqlln sqlda index))))
;-----------------------------------------------------------------------------------

(defun alloc-vars-data (sqlda)
  (loop for i from 0 to (- (cffi:foreign-slot-value sqlda 'xsqlda 'sqld) 1) do
       (multiple-value-bind (tp can-nil) (get-var-type sqlda i)
	 (when can-nil
	   (setf (cffi:foreign-slot-value 
		  (cffi:mem-aref 
		   (cffi:foreign-slot-value sqlda 'xsqlda 'sqlvar) 
		   'xsqlvar i) 'xsqlvar 'sqlind) 
		 (cffi:foreign-alloc :short)))
	 (cond ((eq tp ':text) (alloc-var-data-text sqlda i))
	       ((eq tp ':varying) (alloc-var-data-varying sqlda i))
	       ((eq tp ':float) (alloc-var-data-by-type sqlda i tp 0.0))
	   (T (alloc-var-data-by-type sqlda i tp 0))))))
;-----------------------------------------------------------------------------------
(defun get-sql-type (stmt-handle-pointer)
  (let ((status-vector* (make-status-vector))
	(req* (cffi:foreign-alloc :char :initial-element 21))
	(res* (cffi:foreign-alloc :char :count 8 :initial-element 0))
	(st-type nil))
    (isc-dsql-sql-info status-vector* stmt-handle-pointer 1 req* 8 res*)
    (setf st-type (cond ((= (cffi:mem-aref res* :char 3) 1) 'select)
			((= (cffi:mem-aref res* :char 3) 2) 'insert)
			((= (cffi:mem-aref res* :char 3) 3) 'update)
			((= (cffi:mem-aref res* :char 3) 4) 'delete)
			(T nil)))
    (cffi-sys:foreign-free req*)
    (cffi-sys:foreign-free res*)
    st-type))
;-----------------------------------------------------------------------------------
(defun get-var-val-by-type (sqlda index type)
  (cond 
    ((eq type ':text)
     (cffi:foreign-string-to-lisp (cffi:foreign-slot-value (cffi:mem-aref 
							    (cffi:foreign-slot-value sqlda 'xsqlda 'sqlvar) 
							    'xsqlvar index) 
							   'xsqlvar 'sqldata) ))
						   ;; :count (cffi:foreign-slot-value 
						   ;; 	   (cffi:mem-aref 
						   ;; 	    (cffi:foreign-slot-value sqlda 'xsqlda 'sqlvar) 
						   ;; 	    'xsqlvar index) 'xsqlvar 'sqllen)))
    ((eq type ':varying) 
     (cffi:foreign-string-to-lisp  (inc-pointer 
				    (cffi:foreign-slot-value (cffi:mem-aref 
							      (cffi:foreign-slot-value sqlda 'xsqlda 'sqlvar) 
							      'xsqlvar index) 
							     'xsqlvar 'sqldata) 2)
				   :count (mem-aref 
					   (cffi:foreign-slot-value (cffi:mem-aref 
								     (cffi:foreign-slot-value sqlda 'xsqlda 'sqlvar) 
								     'xsqlvar index) 
								    'xsqlvar 'sqldata) :short)))

    (T (cffi:mem-aref 
	(cffi:foreign-slot-value (cffi:mem-aref 
				    (cffi:foreign-slot-value sqlda 'xsqlda 'sqlvar) 
				    'xsqlvar index) 
				 'xsqlvar 'sqldata) type))))
;-----------------------------------------------------------------------------------
(defun is-var-nil (xsqlda* index)
  (if (nth-value 1 (get-var-type xsqlda* index))
      (if (= -1 (cffi:mem-aref (cffi:foreign-slot-value 
				    (cffi:mem-aref 
				     (cffi:foreign-slot-value xsqlda* 'xsqlda 'sqlvar) 
				     'xsqlvar index) 'xsqlvar 'sqlind)
				   :short))
	  T
	  Nil)
      Nil))
;-----------------------------------------------------------------------------------
(defun get-var-val (xsqlda* index)
   (if (is-var-nil xsqlda* index) 
       nil
       (get-var-val-by-type xsqlda* index (nth-value 0 (get-var-type xsqlda* index)))))
;-----------------------------------------------------------------------------------
(defun get-vars-count (xsqlda*)
  (cffi:foreign-slot-value xsqlda* 'xsqlda 'sqld))
;-----------------------------------------------------------------------------------
(defun get-vars-vals-list (xsqlda*)
  (loop for i from 0 to (- (get-vars-count xsqlda*) 1) collect (get-var-val xsqlda* i)))
;-----------------------------------------------------------------------------------
;===================================================================================
;-----------------------------------------------------------------------------------
(define-condition fb-error (error)
  ((fb-error-code :initarg :fb-error-code :reader fb-error-code)
   (fb-error-text :initarg :fb-error-text :reader fb-error-text)
   (fbclient-msg :initarg :fbclient-msg :reader fbclient-msg)))
;-----------------------------------------------------------------------------------
(defgeneric fb-verbalize-error (err))
(defmethod fb-verbalize-error ((err fb-error))
  (format nil "!fb-error:~%~tcode: ~a~%~ttext: ~a~%~tfbclient-msg: ~a" 
	  (cl-fbclient:fb-error-code err)
	  (cl-fbclient:fb-error-text err)
	  (cl-fbclient:fbclient-msg err)))
;-----------------------------------------------------------------------------------
;; (define-condition fb-dsql-error (fb-error)
;;   ((fbclient-dsql-msg :initarg :fbclient-dsql-msg :reader fbclient-dsql-msg)))
;; ;-----------------------------------------------------------------------------------
;; (defmethod fb-verbalize-error ((err fb-dsql-error))
;;   (format nil "!fb-error:~%~tcode: ~a~%~ttext: ~a~%~tfbclient-msg: ~a~%~tfbclient-dsql-msg: ~a" 
;; 	  (cl-fbclient:fb-error-code err)
;; 	  (cl-fbclient:fb-error-text err)
;; 	  (cl-fbclient:fbclient-msg err)
;; 	  (fbclient-dsql-msg err)))
;-----------------------------------------------------------------------------------
(defun status-vector-error-p (status-vector*)
  (and (= (cffi:mem-aref status-vector* :long 0) 1)
       (/= (cffi:mem-aref status-vector* :long 1) 0)))
;-----------------------------------------------------------------------------------
(defun get-status-vector-sql-msg (status-vector*)
  (let ((msg* (cffi:foreign-alloc :char :initial-element 0 :count 1024))
	(sql-code (isc-sqlcode status-vector*)))
    (unwind-protect 
	 (if (/= sql-code -999) (progn
				  (isc-sql-interprete sql-code msg* 1024)
				  (format nil "~%(DSQL code: ~a): ~a" sql-code (cffi:foreign-string-to-lisp msg*)))
	     "")
      (cffi-sys:foreign-free msg*))))
;-----------------------------------------------------------------------------------
(defun get-status-vector-msg (status-vector*)
  (let ((msg* (cffi:foreign-alloc :char :initial-element 0 :count 512))
	(sv** (cffi:foreign-alloc :pointer :initial-element status-vector*)))
    (let ((sz (isc-interprete msg* sv**)))
    (unwind-protect 
	 (format nil "~a~a"
		 (cffi:foreign-string-to-lisp msg*) 
		 (get-status-vector-sql-msg status-vector*))
      (cffi-sys:foreign-free msg*)))))
;-----------------------------------------------------------------------------------
;===================================================================================
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
(defgeneric fb-connect (fb-db) 
  (:documentation "connecting to fb DB"))
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
(defgeneric fb-disconnect (db)
  (:documentation "disconnecting from DB"))
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
;-----------------------------------------------------------------------------------
(defclass fb-transaction ()
  ((fb-db :accessor fb-db
	  :initarg :fb-db)
   (transaction-handle* :accessor transaction-handle*)))
;-----------------------------------------------------------------------------------
(defgeneric fb-start-transaction (transaction))
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
(defgeneric fb-commit-transaction (transaction))
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
(defgeneric fb-rollback-transaction (transaction))
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
(defgeneric fb-prepare-and-execute-statement (statement))
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
     (when (eq (fb-get-sql-type fb-stmt) 'select)
       (isc-dsql-set-cursor-name status-vector* 
				 (statement-handle* fb-stmt)
				 (cffi:foreign-string-alloc "dyn_cursor") 0)
       (when (status-vector-error-p status-vector*)
	 (unwind-protect
	      (error 'fb-error 
		     :fb-error-code 34 
		     :fb-error-text "Unable to make cursor"
		     :fbclient-msg (get-status-vector-msg status-vector*))
	   (cffi-sys:foreign-free status-vector*))))
     (cffi-sys:foreign-free status-vector*)))
;-----------------------------------------------------------------------------------
(defmethod initialize-instance :after ((stmt fb-statement) &key (no-auto-prepare-and-execute Nil))
  (when (null no-auto-prepare-and-execute) (fb-prepare-and-execute-statement stmt)))
;-----------------------------------------------------------------------------------
(defgeneric fb-statement-free (statement))
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
(defgeneric fb-statement-fetch (statement))
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
(defgeneric fb-statement-get-var-val (statement index))
(defmethod fb-statement-get-var-val ((stmt fb-statement) index)
  (get-var-val (xsqlda-output* stmt) index))
;-----------------------------------------------------------------------------------
(defgeneric fb-statement-get-vars-vals-list (statement))
(defmethod fb-statement-get-vars-vals-list ((stmt fb-statement))
  (get-vars-vals-list (xsqlda-output* stmt)))
;-----------------------------------------------------------------------------------
;===================================================================================
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
(defgeneric fb-noresult-query (fb-db request-str))
(defmethod fb-noresult-query ((fb-db fb-database) request-str)
  (fb-with-statement-db (fb-db st-tmp-name request-str) nil))
;-----------------------------------------------------------------------------------
(defgeneric fb-query-fetch-all (fb-db request-str))
(defmethod fb-query-fetch-all ((fb-db fb-database) request-str)
  (fb-with-statement-db (fb-db st-tmp-name request-str)
			(loop while (fb-statement-fetch st-tmp-name)
			     collect (fb-statement-get-vars-vals-list st-tmp-name))))
;-----------------------------------------------------------------------------------
;===================================================================================
;-----------------------------------------------------------------------------------
;;TEMP
(defun prnt-fbuff (buff sz)
  (loop for i from 0 to (- sz 1) 
     do (format t "~a: '~a' (code: ~a) ~%" i 
		(code-char (cffi:mem-aref buff :char i))
		(cffi:mem-aref buff :char i))))
;-----------------------------------------------------------------------------------
(defmacro test-make-all ()
  '(progn (defparameter *db* (make-db-handler))
  (defparameter *tr* (make-tr-handler))
  (defparameter *stv* (make-status-vector))
  (defparameter *stmt* (make-stmt-handler))))
