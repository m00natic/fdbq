;;;; spec.lisp

(in-package #:fdbq)

(defvar *dbs* (make-hash-table :test 'eq))

(defclass db-field ()
  ((offset :accessor db-field-offset :initarg :offset)
   (size :accessor db-field-size :initarg :size)))

(defclass operand-traits (db-field)
  ((filter :accessor literal-filter :initarg :filter :initform nil)))

(defstruct spec
  (size 0 :type fixnum)
  (type :file :type keyword :read-only t)
  (path #P"" :type pathname :read-only t)
  (fields (make-hash-table :test 'eq) :type hash-table))

(defun get-spec (db)
  (gethash db *dbs*))

(defun field-offset (field spec)
  (db-field-offset (gethash field (spec-fields spec))))

(defun field-size (field spec)
  (db-field-size (gethash field (spec-fields spec))))

(defmacro defspec ((name type path) &rest fields)
  "Register db with NAME, TYPE (only :file supported for now) and file PATH.
FIELDS is a list of ordered column declarations.  Field declaration may be
a list with 2 elements - field name and byte count.  Or may be just a number
signifying unnamed number of filler bytes."
  `(let* ((spec (make-spec :type ,type :path (merge-pathnames ,path)))
          (spec-fields (spec-fields spec))
          (size 0))
     (dolist (field-spec ',fields)
       (if (numberp field-spec)
           (incf size field-spec)
           (let ((field (make-instance 'db-field :offset size
                                                 :size (second field-spec))))
             (setf (gethash (first field-spec) spec-fields) field)
             (incf size (db-field-size field)))))
     (setf (spec-size spec) size
           (gethash ',name *dbs*) spec)))

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
    (make-instance 'operand-traits :offset (db-field-offset field)
                                   :size (db-field-size field))))
