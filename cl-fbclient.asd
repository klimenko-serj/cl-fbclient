;;;; cl-fbclient.asd

(asdf:defsystem #:cl-fbclient
  :serial t
  :description "Common Lisp library for working with firebird databases(using fbclient)"
  :author "Klimenko Serj <klimenko.serj@gmail.com>"
  :license ""
  :depends-on (#:cffi
	       #:local-time)
  :components ((:file "package")
	       (:file "cl-fbclient-cffi")
	       (:file "cl-fbclient-functions")
               (:file "cl-fbclient")))

