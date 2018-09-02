;;;; storage-file.lisp

(in-package #:fdbq)

(defvar *buffer-size* (expt 1024 2)
  "Bytes per job.")

(defclass spec-file (spec)
  ((path :type pathname :accessor spec-path :initarg :path)))

(defmethod defspec (name (type (eql :file)) fields &key path)
  "Register file DB with NAME, list of FIELDS and file PATH."
  (let ((spec (make-instance 'spec-file :path (merge-pathnames path))))
    (defspec-fields spec fields)
    (setf (gethash name *dbs*) spec)))

(defmethod gen-do-lines ((spec spec-file) line-var body
                         &key buffer-var offset-var
                           result-var result-type result-initarg (jobs 1) reduce-fn)
  "File db iteration."
  (let* ((entry-size (spec-size spec))  ;entry size is known
         (line-size (1+ entry-size))    ;add 1 for newline
         (buffer-size (let ((cache-size (* jobs *buffer-size*)))
                        (if (< line-size cache-size)
                            (* line-size (floor cache-size line-size))
                            line-size)))
         (use-only-line? (null buffer-var))
         (take-count (gensym)))
    (unless buffer-var
      (setf buffer-var (gensym)
            offset-var (gensym)))
    (unless result-var
      (setf result-var (gensym)))
    `(let ((,buffer-var (make-array ,buffer-size :element-type 'ascii:ub-char)))
       (flet ((mapper (,offset-var ,take-count)
                (declare (optimize (speed 3) (debug 0) (safety 0) (compilation-speed 0))
                         (type fixnum ,offset-var ,take-count))
                (let ((,line-var (make-string ,entry-size :element-type 'base-char))
                      (,result-var ,result-initarg))
                  (declare ,(if result-type
                                `(type ,result-type ,result-var)
                                `(ignorable ,result-var))
                           (dynamic-extent ,line-var) ;use stack allocation if possible
                           (ignorable ,line-var))
                  (loop for ,offset-var fixnum from ,offset-var by ,line-size
                        repeat ,take-count
                        do (progn
                             ,(when use-only-line?
                                `(loop for i fixnum from 0 below ,entry-size
                                       for j fixnum from ,offset-var
                                       do (setf (aref ,line-var i)
                                                (code-char (aref ,buffer-var j)))))
                             ,@body))
                  ,result-var)))
         (let* ((result ,result-initarg)
                (lparallel:*kernel* (lparallel:make-kernel ,jobs))
                (chan (lparallel:make-channel)))
           (declare ,(if result-type
                         `(type ,result-type result)
                         `(ignorable result))
                    (ignorable chan))
           (unwind-protect
                (with-open-file (ins ,(spec-path spec) :direction :input
                                                       :element-type 'ascii:ub-char)
                  (loop for bytes fixnum = (read-sequence ,buffer-var ins)
                        until (zerop bytes)
                        do ,(cond ((< 1 jobs)
                                   `(progn
                                      (multiple-value-bind (take-count correction)
                                          (ceiling (the fixnum (/ bytes ,line-size)) ,jobs)
                                        (declare (type fixnum take-count correction))
                                        (let ((batch-size (* take-count ,line-size)))
                                          (loop for offset fixnum from 0 by batch-size
                                                repeat ,(1- jobs)
                                                do (lparallel:submit-task chan #'mapper
                                                                          offset take-count))
                                          (lparallel:submit-task chan #'mapper
                                                                 (* ,(1- jobs) batch-size)
                                                                 (+ take-count correction))))
                                      (lparallel:do-fast-receives (res chan ,jobs)
                                        ,(when reduce-fn
                                           `(setf result (,reduce-fn result
                                                                     ,(if result-type
                                                                          `(the ,result-type res)
                                                                          'res)))))))
                                  (reduce-fn
                                   `(setf result (,reduce-fn result
                                                             (mapper 0 (floor bytes
                                                                              ,line-size)))))
                                  (t `(mapper 0 (floor bytes ,line-size))))))
             (lparallel:end-kernel))
           result)))))
