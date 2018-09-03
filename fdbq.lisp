;;;; fdbq.lisp

(in-package #:fdbq)

(defun select* (field-list db &key where print (jobs 1))
  "Select FIELD-LIST from DB with WHERE filter."
  (funcall (compile nil (gen-select (get-spec db) field-list where print jobs))))

(defmacro select (field-list db &key where (print t) (jobs 1))
  "Select FIELD-LIST from DB with WHERE filter."
  `(funcall (compile nil ,(gen-select (get-spec db) field-list where print jobs))))

(defun cnt* (db &key where (jobs 1))
  "Count FIELD-LIST from DB with WHERE filter."
  (funcall (compile nil (gen-cnt (get-spec db) where jobs))))

(defmacro cnt (db &key where (jobs 1))
  "Count FIELD-LIST from DB with WHERE filter."
  `(funcall (compile nil ,(gen-cnt (get-spec db) where jobs))))

