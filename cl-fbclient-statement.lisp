;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; cl-fbclient-statement.lisp
;;;; STATEMENT
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(in-package #:cl-fbclient)

;===================================================================================
;-----------------------------------------------------------------------------------
(defun make-stmt-handler ()
  (cffi:foreign-alloc 'isc_stmt_handle :initial-element 0))
;-----------------------------------------------------------------------------------
(defun XSQLDA-length (n)
  (+ (cffi:foreign-type-size '(:struct XSQLDA)) 
     (* (- n 1) (cffi:foreign-type-size '(:struct XSQLVAR)))))
;-----------------------------------------------------------------------------------
(defun make-xsqlda (n)
  (let ((new-xsqlda (cffi:foreign-alloc :char :count (XSQLDA-length n))))
					;;:initial-element 0))) speed optimization?
     (setf (cffi:foreign-slot-value new-xsqlda '(:struct xsqlda) 'version) 1
	   (cffi:foreign-slot-value new-xsqlda '(:struct xsqlda) 'sqln) n)
     new-xsqlda))
;-----------------------------------------------------------------------------------
(defun need-remake-xsqlda (tmp-xsqlda)
  (> (cffi:foreign-slot-value tmp-xsqlda '(:struct xsqlda) 'sqld) 
     (cffi:foreign-slot-value tmp-xsqlda '(:struct xsqlda) 'sqln)))
;-----------------------------------------------------------------------------------
(defun remake-xsqlda (tmp-xsqlda)
  (unwind-protect 
       (make-xsqlda (cffi:foreign-slot-value tmp-xsqlda '(:struct xsqlda) 'sqld))
    (cffi-sys:foreign-free tmp-xsqlda)))
;-----------------------------------------------------------------------------------
(defun get-var-type-by-fbtype-num (type-num)
      (case type-num
	   (496 ':int)
	   (500 ':short)
	   (482 ':float)
	   (480 ':double)
	   (452 ':text)
	   (448 ':varying)
	   (510 ':timestamp)
	   (580 ':decimal)
	   (520 ':blob)
	   ;...
	   ;TODO: other types
	   (T (format t "Uncknown type #~A!~%" type-num))
	   ))
;-----------------------------------------------------------------------------------
(defmacro %var (%xsqlda %index)
  `(cffi:mem-aptr 
    (cffi:foreign-slot-pointer ,%xsqlda '(:struct xsqlda) 'sqlvar)
    '(:struct xsqlvar) ,%index))
(defmacro %var-slot (%xsqlda %index %slot-name)
  `(cffi:foreign-slot-value 
    (%var ,%xsqlda ,%index)
    '(:struct xsqlvar) ,%slot-name))
;-----------------------------------------------------------------------------------
(defun get-var-type (xsqlda* index)
  (let ((tp (%var-slot xsqlda* index 'sqltype))
	(can-nil T))
    (if (oddp tp) 
        (decf tp)
        (setf can-nil nil))
    (values-list (list (get-var-type-by-fbtype-num tp) can-nil))))
;-----------------------------------------------------------------------------------
(defun get-var-sqlln (xsqlda* index)
  (cffi:foreign-slot-value 
   (%var xsqlda* index)
   '(:struct xsqlvar) 'sqllen))
;-----------------------------------------------------------------------------------
(defun alloc-var-data-default (xsqlda* index)
  (setf (%var-slot xsqlda* index 'sqldata) 
	(cffi:foreign-alloc :char 
			    :initial-element 0 
			    :count (get-var-sqlln xsqlda* index))))
;-----------------------------------------------------------------------------------
(defmacro %vars-count-1 (%xsqlda)
  `(- (cffi:foreign-slot-value ,%xsqlda '(:struct xsqlda) 'sqld) 1))

(defun alloc-vars-data (xsqlda*)
  (loop for i from 0 to (%vars-count-1 xsqlda*) do
       (let ((can-nil (nth-value 1 (get-var-type xsqlda* i)))) ;; TODO: -> when
	 (when can-nil
	   (setf (%var-slot xsqlda* i 'sqlind) 
		 (cffi:foreign-alloc :short)))
	 (alloc-var-data-default xsqlda* i))))
;-----------------------------------------------------------------------------------
(defun free-vars-data (xsqlda*)
  (loop for i from 0 to (%vars-count-1 xsqlda*) do
       (let ((can-nil (nth-value 1 (get-var-type xsqlda* i))))
	 (when can-nil
	   (cffi-sys:foreign-free (%var-slot xsqlda* i 'sqlind)))
	 (cffi-sys:foreign-free (%var-slot xsqlda* i 'sqldata)))))
;-----------------------------------------------------------------------------------
(defun get-sql-type (stmt-handle-pointer)
  (let ((status-vector* (make-status-vector))
	(req* (cffi:foreign-alloc :char :initial-element 21)) 
	(res* (cffi:foreign-alloc :char :count 8 :initial-element 0))
	(st-type nil))
    (isc-dsql-sql-info status-vector* stmt-handle-pointer 1 req* 8 res*)
    (setf st-type (case (cffi:mem-aref res* :char 3)
                    (1 'select)
                    (2 'insert)
                    (3 'update)
                    (4 'delete)
                    (T nil)))
    (cffi-sys:foreign-free req*) ; mem free.
    (cffi-sys:foreign-free res*) ; mem free.
    (cffi-sys:foreign-free status-vector*) ; mem free.
    st-type))
;-----------------------------------------------------------------------------------
(defun xsqlda-get-var-val (xsqlda* index)
  (%var-slot xsqlda* index 'sqldata))
;-----------------------------------------------------------------------------------
(defun xsqlda-get-var-sqlscale (xsqlda* index)
  (%var-slot xsqlda* index 'sqlscale))
;-----------------------------------------------------------------------------------
(defparameter +mulp-vector+ #(1 1e-1 1e-2 1e-3 1e-4 1e-5 1e-6 1e-7 1e-8 1e-9 1e-10
				 1e-11 1e-12 1e-13 1e-14 1e-15 1e-16 1e-17 1e-18 1e-19 1e-20))
(defun pow-10 (n)
  (elt +mulp-vector+ (- n)))
;-----------------------------------------------------------------------------------
(defun fb-timestamp2datetime-list (fb-timestamp)
  (let ((ttm (cffi:foreign-alloc '(:struct tm))))
    (unwind-protect
         (progn
           (isc-decode-timestamp fb-timestamp ttm)
           (with-foreign-slots ((sec min hour mday mon year) ttm (:struct tm))
             (list  :year (+ 1900 year)  :mon (+ 1 mon) :mday mday :hour hour :min min :sec sec)))
      (cffi-sys:foreign-free ttm)))) ; mem free.
;-----------------------------------------------------------------------------------
(defun timestamp-alist-to-string (timestamp-alist)
  (format nil
	  "~A.~A.~A ~A:~A:~A"
	  (getf timestamp-alist :mday)
	  (getf timestamp-alist :mon)
	  (getf timestamp-alist :year)
	  (getf timestamp-alist :hour)
	  (getf timestamp-alist :min)
	  (getf timestamp-alist :sec)))
;-----------------------------------------------------------------------------------
(defparameter *timestamp-alist-converter* #'timestamp-alist-to-string)
;-----------------------------------------------------------------------------------
(defun convert-timestamp-alist (timestamp-alist)
  (if *timestamp-alist-converter*
      (funcall *timestamp-alist-converter* timestamp-alist)
      timestamp-alist))
;-----------------------------------------------------------------------------------
(defun get-var-val-by-type (xsqlda* index type)
  (cond  ; case??
    ((eq type ':text)
     (cffi:foreign-string-to-lisp (xsqlda-get-var-val xsqlda* index)))
    ((eq type ':varying) 
     (cffi:foreign-string-to-lisp  (inc-pointer (xsqlda-get-var-val xsqlda* index) 2)
				   :count (mem-aref (xsqlda-get-var-val xsqlda* index)
						     :short)))
    ((eq type ':timestamp)
     (convert-timestamp-alist
      (fb-timestamp2datetime-list (mem-aptr (xsqlda-get-var-val xsqlda* index) 
                                            '(:struct isc_timestamp)))))
    ((eq type ':decimal)
     (* (cffi:mem-aref (xsqlda-get-var-val xsqlda* index) :long) 
	(pow-10 (xsqlda-get-var-sqlscale xsqlda* index))))
    ((eq type ':blob)
     (cffi:mem-aref (xsqlda-get-var-val xsqlda* index) '(:struct ISC_QUAD)))
    (T (cffi:mem-aref (xsqlda-get-var-val xsqlda* index) type))))
;-----------------------------------------------------------------------------------
(defun is-var-nil (xsqlda* index)
  (and (nth-value 1 (get-var-type xsqlda* index))
       (= -1 (cffi:mem-aref (%var-slot xsqlda* index 'sqlind) :short))))
;-----------------------------------------------------------------------------------
(defun get-var-val (xsqlda* index)
   (unless (is-var-nil xsqlda* index) 
     (get-var-val-by-type xsqlda* index (nth-value 0 (get-var-type xsqlda* index)))))
;-----------------------------------------------------------------------------------
(defun get-var-name (xsqlda* index)
  (cffi:foreign-string-to-lisp 
   (%var-slot xsqlda* index 'sqlname)
   :count (%var-slot xsqlda* index 'sqlname_length)))
;-----------------------------------------------------------------------------------
(defun get-var-val+name (xsqlda* index)
  (list (intern (get-var-name xsqlda* index) "KEYWORD")
	(get-var-val xsqlda* index)))
;-----------------------------------------------------------------------------------
(defun get-vars-names (xsqlda*)
  (loop for i from 0 to (- (get-vars-count xsqlda*) 1) collect (get-var-name xsqlda* i)))
;-----------------------------------------------------------------------------------
(defun get-vars-count (xsqlda*)
  (cffi:foreign-slot-value xsqlda* '(:struct xsqlda) 'sqld))
;-----------------------------------------------------------------------------------
(defun get-vars-vals-list (xsqlda*)
  (loop for i from 0 to (- (get-vars-count xsqlda*) 1) collect (get-var-val xsqlda* i)))
;-----------------------------------------------------------------------------------
(defun get-vars-vals+names-list (xsqlda* &optional (names Nil))
  (let ((max-index (- (get-vars-count xsqlda*) 1)))
    (if names
        (loop for i from 0 to max-index 
              append (if (nth i names)
                         (list (nth i names) (get-var-val xsqlda* i))
                         (get-var-val+name xsqlda* i)))
        (loop for i from 0 to max-index 
              append (get-var-val+name xsqlda* i)))))
;-----------------------------------------------------------------------------------
;===================================================================================

;;===================================================================================
;; FB-STATEMENT
;;-----------------------------------------------------------------------------------
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
;;-----------------------------------------------------------------------------------
(defun fb-allocate-statement (fb-stmt)
  "Method to allocate statement."
  (with-status-vector status-vector*
    (isc-dsql-allocate-statement status-vector*
				 (db-handle* (fb-db (fb-tr fb-stmt)))
				 (statement-handle* fb-stmt))
    (process-status-vector status-vector* 30 "Unable to allocate statement")))
;;-----------------------------------------------------------------------------------
(defun fb-prepare-statement (fb-stmt)
  "Method to prepare statement."
  (with-status-vector status-vector*
    (cffi:with-foreign-string (query-str* (request-str fb-stmt))
      (isc-dsql-prepare status-vector*
			(transaction-handle* (fb-tr fb-stmt))
			(statement-handle* fb-stmt)
			(length (request-str fb-stmt)) 
			query-str*
			0 ; 1 ??
			(cffi:null-pointer)))
    (process-status-vector status-vector* 
			   31 (format nil "Unable to prepare statement: ~a"
						     (request-str fb-stmt))))
  ;; sql_info: query type
  (setf (st-type fb-stmt) (get-sql-type (statement-handle* fb-stmt)))
  
  ;; SELECT - query
  (when (eq (fb-get-sql-type fb-stmt) 'select)
    (setf (xsqlda-output* fb-stmt) (make-xsqlda 10))
    (with-status-vector status-vector*
      (isc-dsql-describe status-vector* 
			 (statement-handle* fb-stmt)
			 1 
			 (xsqlda-output* fb-stmt))
      (process-status-vector status-vector* 
			     32 "Error in isc-dsql-describe"))
    (when (need-remake-xsqlda (xsqlda-output* fb-stmt))
      (setf (xsqlda-output* fb-stmt) (remake-xsqlda (xsqlda-output* fb-stmt)))
      (with-status-vector status-vector*
	(isc-dsql-describe status-vector* 
			   (statement-handle* fb-stmt)
			   1 
			   (xsqlda-output* fb-stmt))
	(process-status-vector status-vector* 
			       32 "Error in isc-dsql-describe")))
    (alloc-vars-data (xsqlda-output* fb-stmt))))
;;-----------------------------------------------------------------------------------
(defun fb-execute-statement (fb-stmt)
  "Method to execute statement."
  (with-status-vector status-vector*
    (if (eq (fb-get-sql-type fb-stmt) 'select)
	(isc-dsql-execute status-vector*
			  (transaction-handle* (fb-tr fb-stmt))
			  (statement-handle* fb-stmt)
			  1 
			  (cffi-sys:null-pointer))
	(isc-dsql-execute2 status-vector*
			   (transaction-handle* (fb-tr fb-stmt))
			   (statement-handle* fb-stmt)
			   1 
			   (cffi-sys:null-pointer)
			   (cffi-sys:null-pointer)))
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
;;-----------------------------------------------------------------------------------
(defun fb-statement-free (stmt)
  "Method to free statement."
  (when (xsqlda-output* stmt) 
    (free-vars-data (xsqlda-output* stmt)) ; mem free.
    (cffi-sys:foreign-free (xsqlda-output* stmt))) ; mem free.
  (with-status-vector status-vector*
    (isc-dsql-free-statement status-vector* (statement-handle* stmt) 2) ;DSQL_drop (no DSQL_close!!!)
    (process-status-vector status-vector* 35 "Unable to free statement"))
  (cffi-sys:foreign-free (statement-handle* stmt)))
;;-----------------------------------------------------------------------------------
(defun fb-statement-fetch (stmt)
  "Method to fetch results from executed statement."
  (if (eq (fb-get-sql-type stmt) 'select)
      (with-status-vector status-vector*
      	(let ((fetch-res (isc-dsql-fetch status-vector* (statement-handle* stmt) 
					 1 (xsqlda-output* stmt))))
	  (process-status-vector status-vector* 36 "Unable to fetch statement")
	  (when (= fetch-res 0) T)))
      (error 'fb-error 
		  :fb-error-code 36 
		  :fb-error-text "Unable to fetch statement. Statement type is not SELECT."
		  :fbclient-msg "")))
;;-----------------------------------------------------------------------------------
(defun fb-statement-get-var-val (stmt index)
  "A method for obtaining the values of result variables. Used after Fetch."
  (let ((cffi:*default-foreign-encoding* (encoding (fb-db (fb-tr stmt)))))
    (get-var-val (xsqlda-output* stmt) index)))
;;-----------------------------------------------------------------------------------
(defun fb-statement-get-vars-vals-list (stmt)
  "A method for obtaining the list of values ​​of result variables. Used after Fetch."
  (let ((cffi:*default-foreign-encoding* (encoding (fb-db (fb-tr stmt)))))
    (get-vars-vals-list (xsqlda-output* stmt))))
;;-----------------------------------------------------------------------------------
(defun fb-statement-get-var-val+name (stmt index)
  "A method for obtaining the values and names of result variables. Used after Fetch."
  (let ((cffi:*default-foreign-encoding* (encoding (fb-db (fb-tr stmt)))))
    (get-var-val+name (xsqlda-output* stmt) index)))
;;-----------------------------------------------------------------------------------
(defun fb-statement-get-vars-vals+names-list (stmt &optional (names Nil))
  "A method for obtaining the list of values and names of result variables. Used after Fetch."
  (let ((cffi:*default-foreign-encoding* (encoding (fb-db (fb-tr stmt)))))
    (get-vars-vals+names-list (xsqlda-output* stmt) names)))
;;-----------------------------------------------------------------------------------
(defun fb-statement-get-vars-names-list (stmt)
  "A method for obtaining names of result variables. Used after Fetch."
  (let ((cffi:*default-foreign-encoding* (encoding (fb-db (fb-tr stmt)))))
    (get-vars-names (xsqlda-output* stmt))))
;;===================================================================================
