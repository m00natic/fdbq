;;;; fdbq.asd

(asdf:defsystem #:fdbq
  :description "SQL-like querying over fixed-field DBs."
  :author "Andrey Kotlarski <m00naticus@gmail.com>"
  :license  "BSD-3"
  :version "0.0.1"
  :depends-on (:cl-ppcre #:cl-string-match)
  :serial t
  :components ((:file "package")
               (:file "spec")
               (:file "fdbq")))
