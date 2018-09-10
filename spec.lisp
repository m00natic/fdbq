;;;; spec.lisp

(in-package #:fdbq)

(defvar *dbs* (make-hash-table :test 'eq)
  "Registered DBs.")

(defclass spec-field ()
  ((name :type string :reader field-name :initarg :name)
   (offset :type fixnum :reader field-offset :initarg :offset)
   (size :type fixnum :reader field-size :initarg :size)))

(defclass operand-traits (spec-field)
  ((filter :initarg :filter :initform nil)))

(defclass spec ()
  ((fields :type hash-table :accessor spec-fields
           :initform (make-hash-table :test 'eq))
   (field-list :type simple-array :accessor field-list)
   (size :type fixnum :accessor spec-size :initform 0))
  (:documentation "DB specification base information.
Contains named fields with offsets and sizes."))

(defun get-spec (db)
  (gethash db *dbs*))

(defgeneric defspec (name type fields &key &allow-other-keys)
  (:documentation "Register db with NAME, TYPE and list of FIELDS.
Additional ARGS depend on type."))

(defun defspec-read-fields (spec fields)
  "Fill SPEC schema with FIELDS and inferred entry size."
  (let ((size 0)
        (spec-fields (spec-fields spec)))
    (setf (field-list spec)
          (make-array (count-if #'consp fields)
                      :element-type 'spec-field
                      :initial-contents
                      (loop for field in fields
                            for field-size = (if (numberp field)
                                                 field
                                                 (second field))
                            unless (numberp field)
                              collect (make-instance 'spec-field
                                                     :name (symbol-name (first field))
                                                     :offset size :size field-size)
                            do (incf size field-size)))
          (spec-size spec) size)
    (let ((field-list (field-list spec)))
      (loop for field in fields
            with field-index = 0
            unless (numberp field)
              do (setf (gethash (first field) spec-fields) (aref field-list field-index))
                 (incf field-index)))))

(defgeneric get-operand-traits (operand spec)
  (:documentation "Determine OPERAND dimensions according to SPEC."))

(defmethod get-operand-traits ((operand string) spec)
  "Get dimensions of literal string OPERAND."
  (declare (ignore spec))
  (make-instance 'operand-traits :offset 0
                                 :size (length operand)
                                 :filter operand))

(defmethod get-operand-traits ((operand symbol) spec)
  "Get dimensions of field OPERAND according to SPEC."
  (let ((field (gethash operand (spec-fields spec))))
    (make-instance 'operand-traits :offset (field-offset field)
                                   :size (field-size field))))
