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

(defmethod gen-select ((spec spec-file) field-list where print jobs)
  "Generate selection procedure for FIELD-LIST from DB with WHERE filter."
  `(lambda () (declare (optimize (speed 3) (debug 0) (safety 0) (compilation-speed 0)))
     ,(if (and print (= 1 jobs))
          (gen-do-lines spec 'line
                        `((when ,(or (gen-where where 'line spec 'buffer 'offset) t)
                            ,(gen-print-selection spec field-list 'line
                                                  :buffer-var 'buffer
                                                  :offset-var 'offset)))
                        :buffer-var 'buffer :offset-var 'offset)
          `(let ((res ,(gen-do-lines spec 'line
                                     `((when ,(or (gen-where where 'line spec 'buffer 'offset) t)
                                         ,(gen-list-selection spec field-list 'line 'result
                                                              :buffer-var 'buffer
                                                              :offset-var 'offset)))
                                     :buffer-var 'buffer :offset-var 'offset
                                     :reduce-fn 'nconc :jobs jobs
                                     :result-var 'result :result-initform nil
                                     :result-type 'list)))
             ,(if print
                  (gen-print-select-results 'res (length field-list))
                  'res)))))

(defmethod gen-print-selection ((spec spec-file) fields line-var
                                &key buffer-var offset-var)
  "Unroll selected FIELDS' print statements.
LINE-VAR is symbol representing the current line variable.
SPEC holds field offset details.
BUFFER-VAR is symbol representing the db buffer.
OFFSET-VAR is symbol representing the current offset in the db buffer."
  (declare (ignore line-var))
  `(progn
     ,@(loop for field in fields ;collect print statements in list and splice them
             collect '(write-char #\|)
             collect `(loop for i fixnum from (+ ,offset-var
                                                 ,(field-offset field spec))
                              below (+ ,offset-var ,(+ (field-offset field spec)
                                                       (field-size field spec)))
                            do (write-char (code-char (aref ,buffer-var i)))))
     (format t "|~%")))

(defmethod gen-list-selection ((spec spec-file) fields line-var result
                               &key buffer-var offset-var)
  "Unroll selected FIELDS' gather statements.
LINE-VAR is symbol representing the current line variable.
SPEC holds field offset details."
  (declare (ignore line-var))
  `(let ((res (make-array ,(length fields))))
     ,@(loop for field in fields
             for i from 0
             collect `(setf (aref res ,i)
                            (let ((field-str (make-string ,(field-size field spec)
                                                          :element-type 'base-char)))
                              (loop for i fixnum from 0 below ,(field-size field spec)
                                    for j fixnum from (+ ,offset-var ,(field-offset field spec))
                                    do (setf (aref field-str i) (code-char (aref ,buffer-var j))))
                              field-str)))
     (setf ,result (nconc ,result (list res)))))

(defmethod gen-cnt ((spec spec-file) where jobs)
  "Generate count procedure over DB with WHERE filter over file."
  `(lambda () (declare (optimize (speed 3) (debug 0) (safety 0) (compilation-speed 0)))
     ,(gen-do-lines spec 'line
                    `((when ,(or (gen-where where 'line spec 'buffer 'offset) t)
                        (incf result)))
                    :buffer-var 'buffer :offset-var 'offset
                    :reduce-fn '+ :jobs jobs
                    :result-var 'result :result-initform 0 :result-type 'fixnum)))

(defmethod gen-do-lines ((spec spec-file) line-var body
                         &key (result-var (gensym)) result-type result-initform
                           (jobs 1) reduce-fn
                           (buffer-var (gensym)) (offset-var (gensym)))
  "File db iteration.  BUFFER-VAR and OFFSET-VAR get bound
to raw byte buffer and current line offset within it respectively."
  (let* ((entry-size (spec-size spec))  ;entry size is known
         (line-size (1+ entry-size))    ;add 1 for newline
         (buffer-size (let ((cache-size (* jobs *buffer-size*)))
                        (if (< line-size cache-size)
                            (* line-size (floor cache-size line-size))
                            line-size)))
         (threading? (< 1 jobs))
         (take-count (gensym))
         (job-results (gensym))
         (job-id (gensym)))
    `(let ((,buffer-var (make-array ,buffer-size :element-type 'ascii:ub-char))
           (,job-results ,(when threading?
                            `(make-array ,jobs :element-type '(cons bit ,(or result-type t))
                                               :initial-contents
                                               (list ,@(loop repeat jobs
                                                             collect `(cons 0 ,result-initform)))))))
       (declare ,@(if threading?
                      `((dynamic-extent ,job-results))
                      `((dynamic-extent ,buffer-var)
                        (ignorable ,job-results))))
       (flet ((mapper (,offset-var ,take-count &optional (,job-id 0))
                (declare (optimize (speed 3) (debug 0) (safety 0) (compilation-speed 0))
                         (type fixnum ,offset-var ,take-count)
                         ,(if threading?
                              `(type fixnum ,job-id)
                              `(ignore ,job-id)))
                (let ((,line-var (make-string ,entry-size :element-type 'base-char))
                      (,result-var ,result-initform))
                  (declare ,(if result-type
                                `(type ,result-type ,result-var)
                                `(ignorable ,result-var))
                           (dynamic-extent ,line-var) ;use stack allocation if possible
                           (ignorable ,line-var))
                  (loop for ,offset-var fixnum from ,offset-var by ,line-size
                        repeat ,take-count
                        do ,@body)
                  ,(if threading?
                       `(let ((job-result (aref ,job-results ,job-id)))
                          (setf (car job-result) 1
                                (cdr job-result) ,result-var))
                       result-var))))
         (let* ((result ,result-initform)
                (lparallel:*kernel* ,(when threading?
                                       `(lparallel:make-kernel ,jobs)))
                (chan ,(when threading?
                         `(lparallel:make-channel))))
           (declare ,(if result-type
                         `(type ,result-type result)
                         `(ignorable result))
                    (ignorable chan))
           (unwind-protect
                (with-open-file (ins ,(spec-path spec) :direction :input
                                                       :element-type 'ascii:ub-char)
                  (loop for bytes fixnum = (read-sequence ,buffer-var ins)
                        until (zerop bytes)
                        do ,(cond (threading?
                                   `(progn
                                      (multiple-value-bind (take-count correction)
                                          (ceiling (the fixnum (/ bytes ,line-size)) ,jobs)
                                        (declare (type fixnum take-count correction))
                                        (let ((batch-size (* take-count ,line-size)))
                                          (loop for offset fixnum from 0 by batch-size
                                                for i fixnum from 0 below ,(1- jobs)
                                                do (lparallel:submit-task chan #'mapper
                                                                          offset take-count i))
                                          (lparallel:submit-task chan #'mapper
                                                                 (* ,(1- jobs) batch-size)
                                                                 (+ take-count correction)
                                                                 ,(1- jobs))))
                                      (let ((i 0)) ;reduce thread results in order
                                        (declare (type fixnum i))
                                        (lparallel:do-fast-receives (res chan ,jobs)
                                          (declare (ignore res))
                                          ,(when reduce-fn
                                             `(loop for j fixnum from i below ,jobs
                                                    do (let ((res (aref ,job-results j)))
                                                         #1=(declare (type (cons bit ,(or result-type t)) res))
                                                         (when (zerop (car res))
                                                           (return))
                                                         (incf i)
                                                         #2=(setf result (,reduce-fn result (cdr res))
                                                                  (car res) 0)))))
                                        (loop for j fixnum from i below ,jobs
                                              do (let ((res (aref ,job-results j)))
                                                   #1# #2#)))))
                                  (reduce-fn
                                   `(setf result (,reduce-fn result
                                                             (mapper 0 (floor bytes
                                                                              ,line-size)))))
                                  (t `(mapper 0 (floor bytes ,line-size))))))
             ,(when threading?
                (lparallel:end-kernel)))
           result)))))
