(defproject lein-emr "0.2.0-SNAPSHOT"
  :description "Wrapper over AWS elastic-mapreduce ruby client. Used
  as a lein plugin to create job flows using Amazon Elastic
  MapReduce."
  :url "https://github.com/dpetrovics/lein-emr"
  :repositories {"sonatype"
                 "http://oss.sonatype.org/content/repositories/releases"}
  :dependencies [[org.clojure/tools.cli "0.2.2"]]
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :eval-in-leiningen true)
