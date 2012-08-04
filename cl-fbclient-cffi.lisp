;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; cl-fbclient-functions.lisp
;;;; CFFI-bindings for cl-fbclient
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(in-package #:cl-fbclient)
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
(cffi:defctype isc_date :int)
(cffi:defctype isc_time :unsigned-int)
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
;-----------------------------------------------------------------------------------
(cffi:defcstruct isc_timestamp 
  (timestamp_date isc_date)
  (timestamp_time isc_time))
;-----------------------------------------------------------------------------------
(defcstruct tm
  (sec :int)
  (min :int)
  (hour :int)
  (mday :int)
  (mon  :int)
  (year :int)
  (wday :int)
  (yday :int)
  (isdst  :boolean)
  (zone   :string)
  (gmtoff :long))
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
(cffi:defcfun "isc_decode_timestamp" :void
  (_timestamp :pointer)
  (_struct_tm :pointer))
;-----------------------------------------------------------------------------------
;===================================================================================
