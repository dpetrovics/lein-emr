(ns leiningen.emr
  (:use clojure.tools.cli
        [clojure.string :only (join)])
  (:require [pallet.execute :as execute]))

;;HELPER FUNCTIONS
(defn has [n xs]
  (= n (count xs)))

(defn select-truthy-keys
  [m key-seq]
  (->> (select-keys m key-seq)
       (filter second)
       (into {})))

(defn add-error [m e-string]
  (update-in m [:_errors] conj e-string))

(defn print-errors
  [error-seq]
  (doseq [e error-seq]
    (println e)))

(defn just-one? [m & kwds]
  (let [entries (select-truthy-keys m kwds)]
    (cond (has 1 entries) m
          (has 0 entries)
          (add-error m (str "Please provide one of the following: " kwds))
          :else (add-error m (str "Only one of the following is allowed: "
                                  (keys entries))))))

(defmacro build-validator
  [& validators]
  `(fn [arg-map#]
     (-> arg-map# ~@validators)))

;;CONFIG STUFF THAT NEEDS TO GO ELSEWHERE
(def bootstrap-actions
  ["s3://elasticmapreduce/bootstrap-actions/configurations/latest/memory-intensive"
   "s3://elasticmapreduce/bootstrap-actions/add-swap --args 2048"
   "s3://elasticmapreduce/bootstrap-actions/configure-hadoop --args FIXTHIS"
   "s3://reddconfig/bootstrap-actions/forma_bootstrap_robin.sh"])

(def native-path "/home/hadoop/native")
(def lib-path "/usr/local/fwtools/usr/lib")
(def redd-config-path "s3://reddconfig/bootstrap-actions/config.xml")

(def default-bid-price
  {"large" 0.32
   "cluster-compute" 1.30
   "high-memory" 1.80})

(defn calc-maps-reds
  "Given the type, it will return a map of the number of map tasks and
  reduce tasks using sensible defaults."
  [type mappers reducers]
  (cond
   (= type "large")             {:map-tasks (or mappers 4)
                                 :reduce-tasks (or reducers 2)}
   (= type "high-memory")       {:map-tasks (or mappers 30)
                                 :reduce-tasks (or reducers 24)}
   (= type "cluster-compute")   {:map-tasks (or mappers 22)
                                 :reduce-tasks (or reducers 16)}))

(defn convert-type
  [type]
  (cond
   (= type "large") "m1.large"
   (= type "high-memory") "m2.4xlarge"
   (= type "cluster-compute") "cc1.4xlarge"))

;;put this into an XML file in redd-config path, except for the ones
;;that depend on reduce tasks, map tasks, and node countx
(defn base-props
  [reduce-tasks map-tasks node-count]
  {:hdfs-site {:dfs.datanode.max.xcievers 5096
               :dfs.namenode.handler.count 20
               :dfs.block.size 134217728
               :dfs.support.append true}
   :mapred-site {:io.sort.mb 200
                 :io.sort.factor 40
                 :mapred.reduce.parallel.copies 20
                 :mapred.task.timeout 10000000
                 :mapred.reduce.tasks (int (* reduce-tasks node-count))
                 :mapred.tasktracker.map.tasks.maximum map-tasks
                 :mapred.tasktracker.reduce.tasks.maximum reduce-tasks
                 :mapred.reduce.max.attempts 12
                 :mapred.map.max.attempts 20
                 :mapred.job.reuse.jvm.num.tasks 20
                 :mapred.map.tasks.speculative.execution false
                 :mapred.reduce.tasks.speculative.execution false
                 :mapred.output.direct.NativeS3FileSystem true
                 :mapred.child.java.opts (str "-Djava.library.path="
                                              native-path
                                              " -Xms1024m -Xmx1024m")
                 :mapred.child.env (str "LD_LIBRARY_PATH="
                                        lib-path)}})

(defn parse-emr-config
  "Takes in a config map of hadoop base props (config file settings)
  and returns a string of hadoop properties for use by the ruby
  elastic-mapreduce script."
  [conf-map]
  (->> (mapcat conf-map [:mapred-site :hdfs-site])
       (map (fn [[k v]]
              (format "-s,%s=%s" (name k) v))) 
       (join ",")       
       (format "\"--core-config-file,%s,%s\"" redd-config-path)))


;;SCRIPT GENERATION
(defn boot-emr!
  [{:keys [name type size zone mappers reducers bid on-demand bs-actions] :as m}]
  (let [{:keys [map-tasks reduce-tasks]}
        (calc-maps-reds type mappers reducers)
        hw-id (convert-type type)]
    (execute/local-script
     (elastic-mapreduce --create --alive
                        --name ~name
                        --availability-zone ~zone
                        --ami-version "2.0.5" ;;not sure if we still
                        ;;need this
                        
                        --instance-group master
                        --instance-type ~hw-id
                        --instance-count 1 
                        
                        --instance-group core
                        --instance-type ~hw-id
                        --instance-count ~size
                        ~(if (nil? bid)
                           (if (nil? on-demand)
                             ;;use default bid price
                             (str "--bid-price "
                                  (default-bid-price type)) 
                             "")                    ;;use on-demand
                           (str " --bid-price " bid)) ;;use given price
                        --enable-debugging

                        --bootstrap-action ;;MAKE THIS INTO A LOOP
                        s3://elasticmapreduce/bootstrap-actions/configurations/latest/memory-intensive
                        
                        --bootstrap-action
                        s3://elasticmapreduce/bootstrap-actions/add-swap
                        --args 2048
                        
                        --bootstrap-action
                        s3://elasticmapreduce/bootstrap-actions/configure-hadoop
                        --args ~(parse-emr-config (base-props map-tasks reduce-tasks size))

                        --bootstrap-action
                        s3://reddconfig/bootstrap-actions/forma_bootstrap_robin.sh))))

;;VALIDATORS
(defn size-valid?
  "This step checks that, if size exist in the arg map,
  they're accompanied by a valid size. If this passes, the function acts as
  identity, else an error is added to the map."
  [{:keys [size] :as m}]
  (if (nil? size)
    (add-error m "Please provide a valid cluster size.")
    m))

(defn bidprice-valid?
  "This step checks that, if `start` or `emr` AND 'bid' exist in the
  arg map, they're accompanied by a valid bid-price. If this passes,
  the function acts as identity, else an error is added to the map."
  [{:keys [bid] :as m}]
  (if (and (contains? m :bid) (nil? bid))
    (add-error m "Please provide a valid bid price (must be a number).")
    m))

(defn type-valid?
  "This step checks that the user has given a valid type. If this passes,
  the function acts as identity, else an error is added to the map."
  [{:keys [type] :as m}]
  (if (or (nil? type)
          (not (or (= type "large")
                   (= type "high-memory")
                   (= type "cluster-compute"))))
    (add-error m "Please specify a valid type (large, high-memory, cluster-compute).")
    m))

(defn mappers-reducers-valid?
  [{:keys [mappers reducers] :as m}]
  (cond
   (and (contains? m :mappers) (nil? mappers))
   (add-error m "Invalid mappers option. Please specify a number.")
   (and (contains? m :reducers) (nil? reducers))
   (add-error m "Invalid reducers option. Please specify a number.")
   :else m))

(defn bid-or-ondemand?
  "Checks to make sure the user didn't specify --ondemand and a --bid."
  [{:keys [on-demand bid] :as m}]
  (if (and (not (nil? on-demand))
           (not (nil? bid)))
    (add-error m "You cannot specify --bid and --on-demand. Use on or the other.")
    m))

(def hadoop-validator
  (build-validator
;;   (just-one? :start :stop :emr :jobtracker-ip)
   (size-valid?)
   (type-valid?)
   (mappers-reducers-valid?)
   (bid-or-ondemand?)
   (bidprice-valid?)))

;;PARSING CLI ARGS
(defn parse-hadoop-args
  "Used to parse command line arguments.  Returns a vector containing
  a map of the parsed arguments, a vector of extra arguments that did
  not match known switches, and a documentation banner to provide
  usage instructions. Returns nil if an invalid switch was given. If a
  valid numeric option (size, bid price, mappers or reducers) is
  supplied, it is converted into a number. If an option is not
  specified, it is not put in the map (unless there's a default). If
  an invalid option is given, it is set to nil. Ex output: {:size
  25, :type large, :name dev}"
  [args]
  ;The spec vectors contain string aliases, documentation, a value,
  ;and an optional parse fn
  (try
    (cli args
         ["-n" "--name" "Name of cluster." :default "dev"]
         ["-t" "--type" "Type  cluster." :default "high-memory"]
         ["-s" "--size" "Size of cluster." :parse-fn #(try
                                                        (Long. %)
                                                        (catch Exception _
                                                          nil))]  
         ["-z" "--zone" "Specifies an availability zone."
          :default "us-east-1d"]
         ["-m" "--mappers" "Specifies number of mappers." :parse-fn #(try
                                                                       (Long. %)
                                                                       (catch Exception _
                                                                         nil))] 
         ["-r" "--reducers" "Specifies number of reducers." :parse-fn #(try
                                                                         (Long. %)
                                                                         (catch Exception _
                                                                           nil))]
         ["-b"  "--bid" "Specifies a bid price." :parse-fn #(try
                                                              (Float. %)
                                                              (catch Exception _
                                                                nil))]
         ["-d" "--on-demand" "Uses on demand-pricing for all nodes."]
         ;;         ["--jobtracker-ip" "Print jobtracker IP address?"]
         ;;         ["--start" "Starts the EMR job flow."]
         ;;         ["--emr" "Boots an EMR cluster."]
         ;;         ["--stop" "Kills a pallet cluster."]
         )
    (catch Exception e (do (println (.getMessage e))
                           nil))))

(defn emr
  "Generates a command for the ruby elastic-mapreduce client based
  upon the given command line arguments and executes it. We first
  parse the command line args, display help usage or errors for
  invalid switches (if there were any), then validate the arguments
  map and display error validations (if there were any). If it passes
  everything then we generate/execute the ruby script with the given
  args."
  [project & args]
  (when-let [arg-map (parse-hadoop-args args)] ;;no invalid swich
    (if (= "help" (first (second arg-map)))
      (println (last arg-map)) ;; print usage for 'help'
      (let [val-arg-map (hadoop-validator (first arg-map))]
        (if-let [e-seq (:_errors val-arg-map)]
          (print-errors e-seq) ;;if validation contains error
          (boot-emr! val-arg-map))))))
