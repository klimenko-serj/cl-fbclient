;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; cl-fbclient-database.lisp
;;;; DATABASE
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(in-package #:cl-fbclient)

;;-----------------------------------------------------------------------------------
(defun make-db-handler ()
  (cffi:foreign-alloc 'isc_db_handle :initial-element 0))
;;-----------------------------------------------------------------------------------
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
;;-----------------------------------------------------------------------------------
(defun calc-dpb-size (username password)
  (+ 5 (length username) (length password)))
;;-----------------------------------------------------------------------------------
(defun make-dpb (username password)
  (let ((dpb nil) (dpb-len nil))
    (setf-dpb-buff dpb dpb-len username password)
    dpb))
;;-----------------------------------------------------------------------------------
(defun connect-to-db (db-handle-pointer status-vector-pointer host+path login pass)
  (let ((h+p (cffi:foreign-string-alloc host+path))
	(dpb (make-dpb login pass)))
    (unwind-protect
	 (isc-attach-database status-vector-pointer (length host+path) 
			      h+p 
			      db-handle-pointer
			      (calc-dpb-size login pass)
			      dpb)
      (cffi-sys:foreign-free h+p)))) ; mem 
      ;;(cffi-sys:foreign-free dpb)))) ;; error???
;;-----------------------------------------------------------------------------------
;;===================================================================================
;; FB-DATABASE
;;-----------------------------------------------------------------------------------
(defclass fb-database ()
  ((db-handle* :accessor db-handle*
	       :initform nil)
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
	     :initform "masterkey")
   (encoding :accessor encoding
	     :initarg :encoding
	     :initform :utf-8))
  (:documentation "Class that handles database connection"))
;;-----------------------------------------------------------------------------------
(defun fb-connect (fb-db)
  "Method to connect to the database."
  (unless (db-handle* fb-db)
    (with-status-vector status-vector*
      (let ((host+path (concatenate 'string (host fb-db) ":" (path fb-db))))
	(setf (db-handle* fb-db) (make-db-handler))
	(connect-to-db (db-handle* fb-db) status-vector* host+path (user-name fb-db) (password fb-db))
	(process-status-vector status-vector* 10 (format nil "Unable to connect ('~a')" host+path))))))
;;-----------------------------------------------------------------------------------
(defmethod initialize-instance :after ((db fb-database) &key (no-auto-connect Nil))
  (when (null no-auto-connect) (fb-connect db)))
;;-----------------------------------------------------------------------------------
(defun fb-disconnect (db)
  "Method to disconnect from the database."
  (when (db-handle* db)
    (with-status-vector status-vector*
      (isc-detach-database status-vector* (db-handle* db)) 
      (process-status-vector status-vector* 11 "Error when disconnecting from DB"))
    (cffi-sys:foreign-free (db-handle* db))
    (setf (db-handle* db) Nil)))
;;===================================================================================
