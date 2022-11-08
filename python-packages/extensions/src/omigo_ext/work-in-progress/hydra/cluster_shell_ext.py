from omigo_core import tsv, utils

class ShellExecutorBase:
    def __init__(self, template, default_props = {}):
        self.template = template
        self.default_props = default_props

    def generate_script(self, props = {}):
        # create base script
        script = self.template

        # create new props
        new_props = {}
        for k in self.default_props.keys():
            new_props[str(k)] = str(self.default_props[k])
        for k in props.keys():
            new_props[str(k)] = str(props[k])

        # create a new props with proper prefix
        for k in new_props.keys():
            k1 = "{" + "omigo.prop.{}".format(str(k)) + "}"
            value = str(new_props[k])
            script = script.replace(k1, value)

        # check if there are left over properties
        index = script.find("{omigo.prop.")
        if (index != -1):
            raise Exception("ShellExecutorBase: generate_script: failed to replace all properties: {}, {}".format(script[index:], new_props))
                   
        # return
        return script

class SparkJobShellExecutor(ShellExecutorBase):
    DEFAULT_TEMPLATE = """#!/bin/bash

# export certain conf parameters to be read through System.getProperty()
export OMIGO_ARJUN_INPUT_FILE="{omigo.arjun.input_file}"
export OMIGO_ARJUN_OUTPUT_FILE="{omigo.arjun.output_file}"
export HDFS_PATH=""

spark-submit \\
  --class {omigo.prop.class} \\
  --executor-cores {omigo.prop.executor-cores} \\
  --master "{omigo.prop.master}" \\
  --deploy-mode "{omigo.prop.deploy-mode}" \\
  --executor-memory {omigo.prop.executor-memory} \\
  --num-executors {omigo.prop.num-executors} \\
  --driver-memory {omigo.prop.driver-memory} \\
  --driver-cores {omigo.prop.driver-cores} \\
  --conf "spark.yarn.maxAppAttempts={omigo.prop.spark.yarn.maxAppAttempts}" \\
  --conf "spark.driver.maxResultSize={omigo.prop.spark.driver.maxResultSize}" \\
  --conf "spark.executor.memoryOverhead={omigo.prop.spark.executor.memoryOverhead}" \\
  --conf "spark.speculation={omigo.prop.spark.speculation}" \\
  --conf "spark.dynamicAllocation.enabled={omigo.prop.spark.dynamicAllocation.enabled}" \\
  --conf "spark.sql.hive.caseSensitiveInferenceMode={omigo.prop.spark.sql.hive.caseSensitiveInferenceMode}" \\
  {omigo.prop.jar_location} \\
  {omigo.prop.command-line-args}

# write to local
hadoop fs -text "${HDFS_PATH}${OMIGO_ARJUN_OUTPUT_FILE}/*gz" | gzip -c > ${HYDRA_PATH}${OMIGO_ARJUN_OUTPUT_FILE}
"""

    DEFAULT_PROPS = {
        "executor-cores": "4",
        "master": "yarn",
        "deploy-mode": "client",
        "executor-memory": "4G",
        "num-executors": "10",
        "driver-memory": "16G",
        "driver-cores": "4",
        "spark.yarn.maxAppAttempts": "1",
        "spark.driver.maxResultSize": "4G",
        "spark.executor.memoryOverhead": "4G",
        "spark.speculation": "false",
        "spark.dynamicAllocation.enabled": "false",
        "spark.sql.hive.caseSensitiveInferenceMode": "NEVER_INFER"
    }

    def __init__(self, template = DEFAULT_TEMPLATE, default_props = DEFAULT_PROPS):
        super().__init__(template, default_props = default_props)

# Shell script executor for spark job. TODO: This is WIP
class SparkJobShellExecutorTSV(tsv.TSV):
    # init
    def __init__(self, header, data, url_encoded_template, props, jar_location):
        super().__init__(header, data)
        self.url_encoded_template = url_encoded_template
        self.props = props
        self.jar_location = jar_location

    # this generates the executable script as output
    def execute(self, main_class_col, command_line_template, output_col, prefix = "shell"):
        # method to be used for explode api
        def __execute_inner__(mp):
            # replace template with anything in the data
            template = utils.replace_template_props(mp, utils.url_decode(self.url_encoded_template))

            # set the new properties
            new_props = {}
            new_props["jar_location"] = self.jar_location
            new_props["class"] = mp[main_class_col]
            new_props["command-line-args"] = utils.replace_template_props(mp, command_line_template)

            # result
            result_mp = {}
            spark_shell_executor = SparkJobShellExecutor(template = template)
            result_mp[output_col + ":url_encoded"] = utils.url_encode(spark_shell_executor.generate_script(props = new_props))

            # return
            return [result_mp]

        # return
        return self \
            .explode(".*", __execute_inner__, prefix, collapse = False)
