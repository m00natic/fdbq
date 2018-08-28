;;;; fdbq.lisp

(in-package #:fdbq)

(defconstant +max-buffer-size+ (* 1024 6)
  "Max chunk size to preload in bytes.")

(defmacro select (field-list db &key where)
  "Select FIELD-LIST from DB with WHERE filter."
  (let ((spec (get-spec db)))  ;pull out the specification for this db
    `(funcall (compile nil (lambda ()  ;guarantee execution of a compiled object
                             (declare (optimize (speed 3) (debug 0) (safety 0)
                                                (compilation-speed 0)))
                             (do-lines (line ,spec buffer offset) ;bind buffer/line to db entries
                               ;; if where is empty, condition is considered always satisfied
                               (when ,(or (gen-where where 'line spec 'buffer 'offset) t)
                                 ,(gen-print-selection field-list 'line spec
                                                       'buffer 'offset))))))))

(defmacro do-lines ((line-var spec &optional buffer-var offset-var) &body body)
  "Bind LINE-VAR to each line in SPEC specified source and execute BODY.
If BUFFER-VAR for byte array and OFFSET-VAR for offset within it are supplied,
expose them to BODY as well."
  (when (eq :file (spec-type spec))
    `(do-file-lines (,line-var ,spec ,buffer-var ,offset-var) ,@body)))

(defmacro do-file-lines ((line-var spec &optional buffer-var offset-var) &body body)
  "Bind LINE-VAR to each line in SPEC specified file and execute BODY.
If BUFFER-VAR for byte array and OFFSET-VAR for offset within it are supplied,
expose them to BODY as well."
  (let* ((entry-size (spec-size spec))  ;entry size is known
         (line-size (1+ entry-size))    ;add 1 for newline
         (buffer-size (* line-size (floor +max-buffer-size+ line-size)))
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
               ;; slide offset through the buffer
               do (loop for ,offset-var fixnum from 0 below (* ,line-size
                                                               (floor ,bytes ,line-size))
                        by ,line-size
                        do (progn
                             ,(when use-only-line?
                                `(loop for i fixnum from 0 below ,entry-size
                                       for j fixnum from ,offset-var
                                       do (setf (aref ,line-var i)
                                                (code-char (aref ,buffer-var j)))))
                             ,@body)))))))

(defun gen-print-selection (fields line-var spec
                            &optional buffer-var offset-var)
  "Unroll selected FIELDS' print statements.
LINE-VAR is symbol representing the current line variable.
SPEC holds field offset details.
BUFFER-VAR is symbol representing the db buffer.
OFFSET-VAR is symbol representing the current offset in the db buffer."
  `(progn
     (format t "|")
     ,@(if buffer-var
           (loop for field in fields ;collect print statements in list and splice them
                 collect `(loop for i fixnum from (+ ,offset-var
                                                     ,(field-offset field spec))
                                  below (+ ,offset-var ,(+ (field-offset field spec)
                                                           (field-size field spec)))
                                do (write-char (code-char (aref ,buffer-var i))))
                 collect '(format t "|"))
           (loop for field in fields ;collect print statements in list and splice them
                 collect `(write-string ,line-var nil
                                        :start ,(field-offset field spec)
                                        :end ,(+ (field-offset field spec) ;constant fold
                                                 (field-size field spec)))
                 collect '(format t "|")))
     (format t "~%")))
