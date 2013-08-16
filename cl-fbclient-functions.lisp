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
  (let ((h+p (cffi:foreign-string-alloc host+path))
	(dpb (make-dpb login pass)))
    (unwind-protect
	 (isc-attach-database status-vector-pointer (length host+path) 
			      h+p ;; mem f??
			      db-handle-pointer
			      (calc-dpb-size login pass)
			      dpb) ; mem f??
      (cffi-sys:foreign-free h+p)
      (cffi-sys:foreign-free dpb))))
;-----------------------------------------------------------------------------------
(defun start-transaction (db-handle-pointer transaction-pointer status-vector-pointer)
  (isc-start-transaction status-vector-pointer transaction-pointer 1 
			 db-handle-pointer 0 (cffi-sys:make-pointer 0)))
;-----------------------------------------------------------------------------------
(defun XSQLDA-length (n)
  (+ (cffi:foreign-type-size '(:struct XSQLDA)) 
     (* (- n 1) (cffi:foreign-type-size '(:struct XSQLVAR)))))
;-----------------------------------------------------------------------------------
(defun make-xsqlda (n)
  (let ((new-xsqlda (cffi:foreign-alloc :char :count (XSQLDA-length n) 
					:initial-element 0)))
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
	   ;...
	   ;TODO: other types
	   (T (format t "Uncknown type #~A!~%" type-num))
	   ))
;-----------------------------------------------------------------------------------
(defun get-var-type (xsqlda* index)
  (let ((tp (cffi:foreign-slot-value 
		  (cffi:mem-aptr 
		   (cffi:foreign-slot-pointer xsqlda* '(:struct xsqlda) 'sqlvar)
		   '(:struct xsqlvar) index) 
		  '(:struct xsqlvar) 'sqltype))
	(can-nil T))
    (if (oddp tp) 
        (decf tp)
        (setf can-nil nil))
    (values-list (list (get-var-type-by-fbtype-num tp) can-nil))))
;-----------------------------------------------------------------------------------
(defun get-var-sqlln (xsqlda* index)
  (cffi:foreign-slot-value 
   (cffi:mem-aptr 
    (cffi:foreign-slot-pointer xsqlda* '(:struct xsqlda) 'sqlvar) 
    '(:struct xsqlvar) index) 
   '(:struct xsqlvar) 'sqllen))
;-----------------------------------------------------------------------------------
(defun alloc-var-data-default (xsqlda* index)
  (setf (cffi:foreign-slot-value 
	 (cffi:mem-aptr 
	  (cffi:foreign-slot-pointer xsqlda* '(:struct xsqlda) 'sqlvar) 
	  '(:struct xsqlvar) index) 
	 '(:struct xsqlvar) 'sqldata) 
	(cffi:foreign-alloc :char 
			    :initial-element 0 
			    :count (get-var-sqlln xsqlda* index))))
;-----------------------------------------------------------------------------------
(defun alloc-vars-data (sqlda)
  (loop for i from 0 to (- (cffi:foreign-slot-value sqlda '(:struct xsqlda) 'sqld) 1) do
       (let ((can-nil (nth-value 1 (get-var-type sqlda i))))
	 (when can-nil
	   (setf (cffi:foreign-slot-value 
		  (cffi:mem-aptr 
		   (cffi:foreign-slot-pointer sqlda '(:struct xsqlda) 'sqlvar) 
		   '(:struct xsqlvar) i) 
		  '(:struct xsqlvar) 'sqlind) 
		 (cffi:foreign-alloc :short)))
	 (alloc-var-data-default sqlda i))))
;-----------------------------------------------------------------------------------
(defun free-vars-data (sqlda)
  (loop for i from 0 to (- (cffi:foreign-slot-value sqlda '(:struct xsqlda) 'sqld) 1) do
       (let ((can-nil (nth-value 1 (get-var-type sqlda i))))
	 (when can-nil
	   (cffi-sys:foreign-free (cffi:foreign-slot-value 
				   (cffi:mem-aptr 
				    (cffi:foreign-slot-pointer sqlda '(:struct xsqlda) 'sqlvar) 
				    '(:struct xsqlvar) i) 
				   '(:struct xsqlvar) 'sqlind)))
	 (cffi-sys:foreign-free (cffi:foreign-slot-value 
				 (cffi:mem-aptr 
				  (cffi:foreign-slot-pointer sqlda '(:struct xsqlda) 'sqlvar) 
				  '(:struct xsqlvar) i) 
				 '(:struct xsqlvar) 'sqldata)))))
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
    (cffi-sys:foreign-free req*)
    (cffi-sys:foreign-free res*)
    st-type))
;-----------------------------------------------------------------------------------
(defun xsqlda-get-var-val (xsqlda* index)
  (cffi:foreign-slot-value (cffi:mem-aptr 
			    (cffi:foreign-slot-pointer xsqlda* '(:struct xsqlda) 'sqlvar) 
			    '(:struct xsqlvar) index) 
			   '(:struct xsqlvar) 'sqldata))
;-----------------------------------------------------------------------------------
(defun xsqlda-get-var-sqlscale (xsqlda* index)
  (cffi:foreign-slot-value (cffi:mem-aptr 
			    (cffi:foreign-slot-pointer xsqlda* '(:struct xsqlda) 'sqlvar) 
			    '(:struct xsqlvar) index) 
			   '(:struct xsqlvar) 'sqlscale))
;-----------------------------------------------------------------------------------
(defparameter +mulp-vector+ #(1 1e-1 1e-2 1e-3 1e-4 1e-5 1e-6 1e-7 1e-8 1e-9 1e-10
				 1e-11 1e-12 1e-13 1e-14 1e-15 1e-16 1e-17 1e-18 1e-19 1e-20))
(defun pow-10 (n)
  (elt +mulp-vector+ (- n)))
;-----------------------------------------------------------------------------------
(defun fb-timestamp2datetime-list (fb-timestamp)
  (let ((ttm (cffi:foreign-alloc '(:struct tm))))
    (isc-decode-timestamp fb-timestamp ttm)
     (with-foreign-slots ((sec min hour mday mon year) ttm (:struct tm))
       (list  :year (+ 1900 year)  :mon (+ 1 mon) :mday mday :hour hour :min min :sec sec))))
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
  (cond 
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
    (T (cffi:mem-aref (xsqlda-get-var-val xsqlda* index) type))))
;-----------------------------------------------------------------------------------
(defun is-var-nil (xsqlda* index)
  (and (nth-value 1 (get-var-type xsqlda* index))
       (= -1 (cffi:mem-aref (cffi:foreign-slot-value 
			     (cffi:mem-aptr 
				 (cffi:foreign-slot-pointer xsqlda* '(:struct xsqlda) 'sqlvar) 
				 '(:struct xsqlvar) index) 
				'(:struct xsqlvar) 'sqlind)
			    :short))))
;-----------------------------------------------------------------------------------
(defun get-var-val (xsqlda* index)
   (unless (is-var-nil xsqlda* index) 
     (get-var-val-by-type xsqlda* index (nth-value 0 (get-var-type xsqlda* index)))))
;-----------------------------------------------------------------------------------
(defun get-var-name (xsqlda* index)
  (cffi:foreign-string-to-lisp 
   (cffi:foreign-slot-value 
    (cffi:mem-aptr 
     (cffi:foreign-slot-pointer xsqlda* '(:struct xsqlda) 'sqlvar)
     '(:struct xsqlvar) index)
    '(:struct xsqlvar) 'sqlname)
   :count (cffi:foreign-slot-value 
	   (cffi:mem-aptr 
	    (cffi:foreign-slot-pointer xsqlda* '(:struct xsqlda) 'sqlvar)
	    '(:struct xsqlvar) index)
	   '(:struct xsqlvar) 'sqlname_length)))
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
(defun status-vector-error-p (status-vector*)
  (and (= (cffi:mem-aref status-vector* :long 0) 1)
       (/= (cffi:mem-aref status-vector* :long 1) 0)))
;-----------------------------------------------------------------------------------
(defun get-status-vector-sql-msg (status-vector*)
  (let ((msg* (cffi:foreign-alloc :char :initial-element 0 :count 1024))
	(sql-code (isc-sqlcode status-vector*)))
    (unwind-protect 
	 (if (/= sql-code -999) 
	     (progn
	       (isc-sql-interprete sql-code msg* 1024)
	       (format nil "~%(DSQL code: ~a): ~a" 
		       sql-code (cffi:foreign-string-to-lisp msg*)))
	     "")
      (cffi-sys:foreign-free msg*))))
;-----------------------------------------------------------------------------------
(defun get-status-vector-msg (status-vector*)
  (let ((msg* (cffi:foreign-alloc :char :initial-element 0 :count 512))
	(sv** (cffi:foreign-alloc :pointer :initial-element status-vector*)))
    (unwind-protect 
	 (progn
	   (isc-interprete msg* sv**)
	   (format nil "~a~a"
		   (cffi:foreign-string-to-lisp msg*) 
		   (get-status-vector-sql-msg status-vector*)))
      (cffi-sys:foreign-free msg*)
      (cffi-sys:foreign-free sv**)))) ;; mem free
;-----------------------------------------------------------------------------------
;===================================================================================

