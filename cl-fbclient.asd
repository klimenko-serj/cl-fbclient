;;;; cl-fbclient.asd

(asdf:defsystem #:cl-fbclient
  :serial t
  :description "Describe cl-fbclient here"
  :author "Your Name <your.name@example.com>"
  :license "Specify license here"
  :depends-on (#:cffi)
  :components ((:file "package")
               (:file "cl-fbclient")))

