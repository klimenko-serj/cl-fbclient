;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; cl-fbclient-error.lisp
;;;; ERROR
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(in-package #:cl-fbclient)

;;===================================================================================
;; status_vector
;;===================================================================================
(defun make-status-vector ()
  (cffi:foreign-alloc 'isc_status :count 20))
;;-----------------------------------------------------------------------------------
(defun status-vector-error-p (status-vector*)
  (and (= (cffi:mem-aref status-vector* :long 0) 1)
       (/= (cffi:mem-aref status-vector* :long 1) 0)))
;;-----------------------------------------------------------------------------------
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
      (cffi-sys:foreign-free msg*)))) ; mem free.
;;-----------------------------------------------------------------------------------
(defun get-status-vector-msg (status-vector*)
  (let ((msg* (cffi:foreign-alloc :char :initial-element 0 :count 512))
	(sv** (cffi:foreign-alloc :pointer :initial-element status-vector*)))
    (unwind-protect 
	 (progn
	   (isc-interprete msg* sv**)
	   (format nil "~a~a"
		   (cffi:foreign-string-to-lisp msg*) 
		   (get-status-vector-sql-msg status-vector*)))
      (cffi-sys:foreign-free msg*) ; mem free.
      (cffi-sys:foreign-free sv**)))) ; mem free.
;;-----------------------------------------------------------------------------------
;;===================================================================================
;; FB-ERROR
;;-----------------------------------------------------------------------------------
(define-condition fb-error (error)
  ((fb-error-code :initarg :fb-error-code :reader fb-error-code)
   (fb-error-text :initarg :fb-error-text :reader fb-error-text)
   (fbclient-msg :initarg :fbclient-msg :reader fbclient-msg))
  (:documentation "Condition for processing fbclient errors."))
;;-----------------------------------------------------------------------------------
(defun fb-verbalize-error (err)
  (format nil "!fb-error:~%~tcode: ~a~%~ttext: ~a~%~tfbclient-msg: ~a" 
	  (cl-fbclient:fb-error-code err)
	  (cl-fbclient:fb-error-text err)
	  (cl-fbclient:fbclient-msg err)))
;;-----------------------------------------------------------------------------------
(defmethod print-object ((err fb-error) stream)
  (format stream (fb-verbalize-error err)))
;;-----------------------------------------------------------------------------------
(defmacro with-status-vector (status-vector* &body body)
  `(let ((,status-vector* (make-status-vector))) 
       (unwind-protect
	    ,@body
	 (cffi-sys:foreign-free ,status-vector*)))) ; mem free.
;;-----------------------------------------------------------------------------------
(defmacro process-status-vector (status-vector* err-code err-text)
  `(when (status-vector-error-p ,status-vector*)
     (error 'fb-error 
	    :fb-error-code ,err-code
	    :fb-error-text ,err-text
	    :fbclient-msg (get-status-vector-msg ,status-vector*))))
;;===================================================================================
