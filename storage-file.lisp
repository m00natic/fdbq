;;;; storage-file.lisp

(in-package #:fdbq)

(defconstant +max-buffer-size+ (* 1024 6)
  "Max chunk size to preload in bytes.")

(defclass spec-file (spec)
  ((size :type fixnum :accessor spec-size :initform 0)
   (path :type pathname :accessor spec-path :initarg :path)))

(defmethod defspec (name (type (eql :file)) fields &key path)
  "Register file DB with NAME, list of FIELDS and file PATH."
  (let ((spec (make-instance 'spec-file :path (merge-pathnames path))))
    (setf (spec-size spec) (defspec-fields spec fields)
          (gethash name *dbs*) spec)))

(defmethod gen-do-lines ((spec spec-file) line-var body &key buffer-var offset-var)
  "File line iteration over BODY with LINE-VAR bound to current line string.
If non-nil BUFFER-VAR and OFFSET-VAR bind them to raw byte buffer and
current line offset within it respectively."
  (let* ((entry-size (spec-size spec))  ;entry size is known
         (line-size (1+ entry-size))    ;add 1 for newline
         (buffer-size (if (< line-size +max-buffer-size+)
                          (* line-size (floor +max-buffer-size+ line-size))
                          line-size))
         (ins (gensym)) ;make sure the file stream variable is not visible to the body
         (bytes (gensym))               ;-//- for number of bytes read
         (use-only-line? (null buffer-var)))
    (unless buffer-var
      (setf buffer-var (gensym)
            offset-var (gensym)))
    `(let ((,buffer-var (make-array ,buffer-size :element-type 'ascii:ub-char)) ;allocate read
           (,line-var (make-string ,entry-size :element-type 'base-char))) ;and line buffers
       (declare (type (simple-array ascii:ub-char (,buffer-size)) ,buffer-var)
                (type (simple-base-string ,entry-size) ,line-var) ;no need for newline
                (dynamic-extent ,buffer-var ,line-var)) ;use stack allocation if possible
       (with-open-file (,ins ,(spec-path spec) :direction :input
                                               :element-type 'ascii:ub-char)
         (loop for ,bytes fixnum = (read-sequence ,buffer-var ,ins) ;read as many lines
               until (zerop ,bytes)
               do ,(cond             ; slide offset through the buffer
                     ((/= line-size buffer-size)
                      `(loop for ,offset-var fixnum from 0 below (* ,line-size
                                                                    (floor ,bytes ,line-size))
                             by ,line-size
                             do (progn
                                  ,(when use-only-line?
                                     `(loop for i fixnum from 0 below ,entry-size
                                            for j fixnum from ,offset-var
                                            do (setf (aref ,line-var i)
                                                     (code-char (aref ,buffer-var j)))))
                                  ,@body)))
                     (use-only-line?
                      `(progn
                         (loop for i fixnum from 0 below ,entry-size
                               do (setf (aref ,line-var i)
                                        (code-char (aref ,buffer-var i))))
                         ,@body))
                     (t `(let ((,offset-var 0))
                           (declare (type fixnum ,offset-var))
                           ,@body))))))))
