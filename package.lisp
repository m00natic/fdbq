;;;; package.lisp

(defpackage #:fdbq
  (:use #:cl)
  (:export #:defspec
           #:gen-do-lines
           #:gen-where
           #:select
           #:select*
           #:cnt
           #:cnt*))
