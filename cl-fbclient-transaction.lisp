;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; cl-fbclient-transaction.lisp
;;;; TRANSACTION
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(in-package #:cl-fbclient)

;;-----------------------------------------------------------------------------------
(defun make-tr-handler ()
  (cffi:foreign-alloc 'isc_tr_handle :initial-element 0))
;;-----------------------------------------------------------------------------------
(defun make-default-tpb ()
  (let ((tpb (cffi:foreign-alloc :char :count 4)))
    (setf (cffi:mem-aref tpb :char 0) 3 ;isc_tpb_version3
	  (cffi:mem-aref tpb :char 1) 9 ;isc_tpb_write
	  (cffi:mem-aref tpb :char 2) 2 ;isc_tpb_concurrency
	  (cffi:mem-aref tpb :char 3) 6) ;isc_tpb_wait
	 ; (cffi:mem-aref tpb :char 4) 14) ;isc_ignore_limbo
    tpb))
;;-----------------------------------------------------------------------------------
(defun start-transaction (db-handle-pointer transaction-pointer status-vector-pointer)
  ;;TODO: TPB(...) 
  (isc-start-transaction status-vector-pointer transaction-pointer 1 
			 db-handle-pointer 0 (cffi-sys:null-pointer))) ; default TPB
;;-----------------------------------------------------------------------------------

;;===================================================================================
;; FB-TRANSACTION
;;-----------------------------------------------------------------------------------
(defclass fb-transaction ()
  ((fb-db :accessor fb-db
	  :initarg :fb-db)
   (transaction-handle* :accessor transaction-handle*
			:initform (make-tr-handler))) ;mem leak??
  (:documentation "Class that handles transaction."))
;-----------------------------------------------------------------------------------
(defun fb-start-transaction (tr) ;TODO: make-tr-handler 
  "Method to start transaction."
  (with-status-vector status-vector*
    (start-transaction (db-handle* (fb-db tr)) (transaction-handle* tr) status-vector*)
    (process-status-vector status-vector* 20 "Unable to start transaction")))
;-----------------------------------------------------------------------------------
(defmethod initialize-instance :after ((tr fb-transaction) &key (no-auto-start Nil))
  (when (null no-auto-start) (fb-start-transaction tr)))
;-----------------------------------------------------------------------------------
(defun fb-commit-transaction (tr)
  "Method to commit transaction."
  (with-status-vector status-vector*
    (isc-commit-transaction status-vector* (transaction-handle* tr)) 
    (process-status-vector status-vector* 21 "Unable to commit transaction"))
  (cffi-sys:foreign-free (transaction-handle* tr)))
;-----------------------------------------------------------------------------------
(defun fb-rollback-transaction (tr)
  "Method to rollback transaction."
  (with-status-vector status-vector*
    (isc-rollback-transaction status-vector* (transaction-handle* tr)) 
    (process-status-vector status-vector* 22 "Unable to rollback transaction"))
  (cffi-sys:foreign-free (transaction-handle* tr)))
;;===================================================================================
