;;;; spec.lisp

(in-package #:fdbq)

(defvar *dbs* (make-hash-table :test 'eq)
  "Registered DBs.")

(defclass db-field ()
  ((offset :type fixnum :accessor db-field-offset :initarg :offset)
   (size :type fixnum :accessor db-field-size :initarg :size)))

(defclass operand-traits (db-field)
  ((filter :accessor literal-filter :initarg :filter :initform nil)))

(defclass spec ()
  ((fields :type hash-table :accessor spec-fields
           :initform (make-hash-table :test 'eq)))
  (:documentation "DB specification base information.
Contains named fields with offsets and sizes."))

(defun get-spec (db)
  (gethash db *dbs*))

(defun field-offset (field spec)
  (db-field-offset (gethash field (spec-fields spec))))

(defun field-size (field spec)
  (db-field-size (gethash field (spec-fields spec))))

(defgeneric defspec (name type fields &key &allow-other-keys)
  (:documentation "Register db with NAME, TYPE and list of FIELDS.
Additional ARGS depend on type."))

(defun defspec-fields (spec fields)
  "Fill SPEC schema with FIELDS and return inferred entry size."
  (let ((spec-fields (spec-fields spec))
        (size 0))
    (dolist (field-spec fields)
      (if (numberp field-spec)
          (incf size field-spec)
          (let ((field (make-instance 'db-field :offset size
                                                :size (second field-spec))))
            (setf (gethash (first field-spec) spec-fields) field)
            (incf size (db-field-size field)))))
    size))

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

(defgeneric gen-do-lines (spec line-var body
                          &key buffer-var offset-var
                            result-var result-type result-initarg
                            jobs reduce-fn &allow-other-keys)
  (:documentation "Generator of generic entry iteration code over BODY
with LINE-VAR bound to current line string.
If non-nil BUFFER-VAR and OFFSET-VAR bind them to raw byte buffer and
current line offset within it respectively."))
