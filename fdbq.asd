;;;; fdbq.asd

(asdf:defsystem #:fdbq
  :description "SQL-like querying over fixed-field DBs."
  :author "Andrey Kotlarski <m00naticus@gmail.com>"
  :license  "BSD-3"
  :version "0.4"
  :depends-on (:cl-ppcre #:ascii-strings #:lparallel #:alexandria)
  :serial t
  :components ((:file "package")
               (:file "spec")
               (:file "filter")
               (:file "utils")
               (:file "storage-file")
               (:file "fdbq")))
