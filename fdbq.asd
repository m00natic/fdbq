;;;; fdbq.asd

(asdf:defsystem #:fdbq
  :description "SQL-like querying over fixed-field DBs."
  :author "Andrey Kotlarski <m00naticus@gmail.com>"
  :license  "BSD-3"
  :version "0.3"
  :depends-on (:cl-ppcre #:ascii-strings #:lparallel)
  :serial t
  :components ((:file "package")
               (:file "spec")
               (:file "filter")
               (:file "storage-file")
               (:file "fdbq")))
