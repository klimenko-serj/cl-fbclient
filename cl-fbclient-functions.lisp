;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; cl-fbclient-functions.lisp
;;;; FUNCTIONS for fb-client
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(in-package #:cl-fbclient)
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
;-----------------------------------------------------------------------------------
(defun remake-xsqlda (tmp-xsqlda)
  (cond ((> (cffi:foreign-slot-value tmp-xsqlda 'xsqlda 'sqld) 
	    (cffi:foreign-slot-value tmp-xsqlda 'xsqlda 'sqln))
	(let ((new-xsqlda (make-xsqlda 
			   (cffi:foreign-slot-value tmp-xsqlda 'xsqlda 'sqld))))
	  (cffi-sys:foreign-free tmp-xsqlda)
	  new-xsqlda))
	(T tmp-xsqlda)))
;-----------------------------------------------------------------------------------
(defun get-var-type-by-fbtype-num (type-num)
      (cond 
	   ((= type-num 496) ':int)
	   ((= type-num 500) ':short)
	   ((= type-num 482) ':float)
	   ((= type-num 480) ':double)
	   ((= type-num 452) ':text)
	   ((= type-num 448) ':varying)
	   ((= type-num 510) ':timestamp)
	   ((= type-num 580) ':decimal)
	   ;...
	   ;TODO: other types
	   (T (format t "Uncknown type #~A!~%" type-num))
	   ))
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
(defun alloc-var-data-default (xsqlda* index)
  (setf (cffi:foreign-slot-value 
	 (cffi:mem-aref 
	  (cffi:foreign-slot-value xsqlda* 'xsqlda 'sqlvar) 
	  'xsqlvar index) 'xsqlvar 'sqldata) 
	(cffi:foreign-alloc :char 
			    :initial-element 0 
			    :count (get-var-sqlln xsqlda* index))))
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
	 (alloc-var-data-default sqlda i))))
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
(defun xsqlda-get-var-val (xsqlda* index)
  (cffi:foreign-slot-value (cffi:mem-aref 
			    (cffi:foreign-slot-value xsqlda* 'xsqlda 'sqlvar) 
			    'xsqlvar index) 
			   'xsqlvar 'sqldata))
;-----------------------------------------------------------------------------------
(defun xsqlda-get-var-sqlscale (xsqlda* index)
  (cffi:foreign-slot-value (cffi:mem-aref 
			    (cffi:foreign-slot-value xsqlda* 'xsqlda 'sqlvar) 
			    'xsqlvar index) 
			   'xsqlvar 'sqlscale))
;-----------------------------------------------------------------------------------
(defparameter +mulp-vector+ #(1 1e-1 1e-2 1e-3 1e-4 1e-5 1e-6 1e-7 1e-8 1e-9 1e-10
				 1e-11 1e-12 1e-13 1e-14 1e-15 1e-16 1e-17 1e-18 1e-19 1e-20))
(defun pow-10 (n)
  (elt +mulp-vector+ (- n)))
;-----------------------------------------------------------------------------------
(defun fb-timestamp2datetime-list (fb-timestamp)
  (let ((ttm (cffi:foreign-alloc 'tm)))
    (isc-decode-timestamp fb-timestamp ttm)
     (with-foreign-slots ((sec min hour mday mon year) ttm tm)
       (list  :year (+ 1900 year)  :mon (+ 1 mon) :mday mday :hour hour :min min :sec sec))))
;-----------------------------------------------------------------------------------
(defparameter *convert-timestamp-to-string* T)
(defparameter *timestamp-string-format* (list ':year "/"  ':mon "/" ':mday " " ':hour ":" ':min ":" ':sec))
(defun datetime-list2string (datetime-list)
  (if *convert-timestamp-to-string*
      (with-output-to-string (ss)
	(loop for x in *timestamp-string-format*
	   do (if (stringp x) 
		  (write-string x ss)
		  (write (getf datetime-list x) :stream ss))))
      datetime-list))
;-----------------------------------------------------------------------------------
(defun get-var-val-by-type (xsqlda* index type)
  (cond 
    ((eq type ':text)
     (cffi:foreign-string-to-lisp (xsqlda-get-var-val xsqlda* index)))
    ((eq type ':varying) 
     (cffi:foreign-string-to-lisp  (inc-pointer (xsqlda-get-var-val xsqlda* index) 2)
				   :count (mem-aref (xsqlda-get-var-val xsqlda* index)
						     :short)))
    ((eq type ':timestamp)
     (datetime-list2string (fb-timestamp2datetime-list (mem-aref (xsqlda-get-var-val xsqlda* index) 
					'isc_timestamp))))
    ((eq type ':decimal)
     (* (cffi:mem-aref (xsqlda-get-var-val xsqlda* index) :long) (pow-10 (xsqlda-get-var-sqlscale xsqlda* index))))
     
    (T (cffi:mem-aref (xsqlda-get-var-val xsqlda* index) type))))
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
(defun get-var-name (xsqlda* index)
  (cffi:foreign-string-to-lisp (cffi:foreign-slot-value 
				(cffi:mem-aref 
				 (cffi:foreign-slot-value xsqlda* 'xsqlda 'sqlvar)
				 'xsqlvar index)
			        'xsqlvar 'sqlname)
			       :count (cffi:foreign-slot-value 
				       (cffi:mem-aref 
					(cffi:foreign-slot-value xsqlda* 'xsqlda 'sqlvar)
					'xsqlvar index)
				       'xsqlvar 'sqlname_length)))
;-----------------------------------------------------------------------------------
(defun get-var-val+name (xsqlda* index)
  (list (get-var-name xsqlda* index)
	(get-var-val xsqlda* index)))
(defun get-vars-names (xsqlda*)
  (loop for i from 0 to (- (get-vars-count xsqlda*) 1) collect (get-var-name xsqlda* i)))
;-----------------------------------------------------------------------------------
(defun get-vars-count (xsqlda*)
  (cffi:foreign-slot-value xsqlda* 'xsqlda 'sqld))
;-----------------------------------------------------------------------------------
(defun get-vars-vals-list (xsqlda*)
  (loop for i from 0 to (- (get-vars-count xsqlda*) 1) collect (get-var-val xsqlda* i)))
;-----------------------------------------------------------------------------------
(defun get-vars-vals+names-list (xsqlda*)
  (loop for i from 0 to (- (get-vars-count xsqlda*) 1) 
       collect (get-var-val+name xsqlda* i)))
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

