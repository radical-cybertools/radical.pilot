
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import subprocess
import sys

from .base import LaunchMethod


# ==============================================================================
#
# The Launch Method Implementation for Running Spark applications
#
class Spark(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, name, cfg, session):

        LaunchMethod.__init__(self, name, cfg, session)

    # --------------------------------------------------------------------------
    #
    @classmethod
    def lrms_config_hook(cls, name, cfg, lrms, logger, profiler=None):

        import radical.utils as ru

        if not os.environ.get('SPARK_HOME'):
            logger.info("Downloading Apache Spark..")
            try:

                VERSION = "2.0.2"
                subprocess.check_call("wget http://d3kbcqa49mib13.cloudfront.net/spark-2.0.2-bin-hadoop2.7.tgz".split())
                subprocess.check_call('tar -xzf spark-2.0.2-bin-hadoop2.7.tgz'.split())
                subprocess.check_call("rm spark-2.0.2-bin-hadoop2.7.tgz ".split())
                subprocess.check_call(("mv spark-2.0.2-bin-hadoop2.7 spark-" + VERSION).split())
            except  Exception as e:
                raise RuntimeError("Spark wasn't installed properly. Please try again. %s " % e )
            spark_home = os.getcwd() + '/spark-' + VERSION
        else:
            spark_home = os.environ['SPARK_HOME']

        # ------------------------------------------------------------------
        platform_os = sys.platform
        java_home = os.environ.get('JAVA_HOME')

        if platform_os == "linux" or platform_os == "linux2":
            if not java_home:
                java = ru.which('java')
                if java != '/usr/bin/java':
                    jpos = java.split('bin')
                else:
                    jpos = os.path.realpath('/usr/bin/java').split('bin')

                if jpos[0].find('jre') != -1:
                    java_home = jpos[0][:jpos[0].find('jre')]
                else:
                    java_home = jpos[0]
        else:
            if not java_home:
                try:
                    java_home = subprocess.check_output("/usr/libexec/java_home").split()[0]
                except Exception:
                    java_home = '/Library/Java/Home'


        spark_conf_slaves = open(spark_home + "/conf/slaves",'w')

        if len(lrms.node_list) == 1:
            spark_conf_slaves.write(lrms.node_list[0])
            spark_conf_slaves.write('\n')
        else:
            for nodename in lrms.node_list[1:]:
                spark_conf_slaves.write(nodename)
                spark_conf_slaves.write('\n')

        spark_conf_slaves.close()

        # put Master Ip in spark-env.sh file - 

        if len(lrms.node_list) == 1:
            master_ip = lrms.node_list[0]
        else:
            try:
                master_ip = subprocess.check_output('hostname -f'.split()).strip()
            except Exception as e:
                raise RuntimeError("Master ip couldn't be detected. %s" % e)

        # Setup default env properties:
        spark_default_file = open(spark_home + "/conf/spark-defaults.conf",'w')
        spark_master_string = 'spark://%s:7077' % master_ip
        spark_default_file.write('spark.master  ' + spark_master_string + '\n')
        spark_default_file.close()
        logger.info("Let's print the config")
        logger.info('Config : {0}'.format(cfg['resource_cfg']))

        spark_env_file = open(spark_home + "/conf/spark-env.sh",'w')
        # load in the spark enviroment of master and slaves the
        # configurations of the machine
        if master_ip != 'localhost':
            for config in cfg['resource_cfg']['pre_bootstrap_0']:
                spark_env_file.write(config + '\n')

        spark_env_file.write('export SPARK_MASTER_HOST=' + master_ip + "\n")
        spark_env_file.write('export JAVA_HOME=' + java_home + "\n")
        spark_env_file.write('export SPARK_LOG_DIR=' + os.getcwd() + '/spark-logs' + '\n')
        # spark_env_file.write('export PYSPARK_PYTHON=`which python` \n')
        spark_env_file.close()


        # Start spark Cluster
        try:
            subprocess.check_output(spark_home + '/sbin/start-all.sh')
        except Exception as e:
            raise RuntimeError("Spark Cluster failed to start: %s" % e)

        logger.info('Start Spark Cluster')
        launch_command = spark_home + '/bin'

        # The LRMS instance is only available here -- everything which is later
        # needed by the scheduler or launch method is stored in an 'lm_info'
        # dict.  That lm_info dict will be attached to the scheduler's lrms_info
        # dict, and will be passed around as part of the slots structure,
        # so it is available on all LM create_command calls.
        lm_info = {'spark_home'    : spark_home,
                   'master_ip'     : master_ip,
                   'lm_detail'     : spark_master_string,
                   'name'          : lrms.name,
                   'launch_command': launch_command,
                   'nodename'      : lrms.node_list[0]}

        return lm_info


    # --------------------------------------------------------------------------
    #
    @classmethod
    def lrms_shutdown_hook(cls, name, cfg, lrms, lm_info, logger, profiler=None):
        if 'name' not in lm_info:
            raise RuntimeError('name not in lm_info for %s' % name)

        if lm_info['name'] != 'SPARKLRMS':
            logger.info('Stoping SPARK')
            stop_spark = subprocess.check_output(lm_info['spark_home'] + '/sbin/stop-all.sh') 
            if 'Error' in stop_spark:
                logger.warn("Spark didn't terminate properly")
            else:
                logger.info("Spark stopped successfully")

        os.remove('spark-2.0.2-bin-hadoop2.7.tgz')


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self._log.info(self._cfg['lrms_info']['lm_info'])
        self.launch_command = self._cfg['lrms_info']['lm_info']['launch_command']
        self._log.info('SPARK was called')

    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        slots        = cu['slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_args    = cud.get('arguments')
        task_env     = cud.get('environment')

        # Construct the args_string which is the arguments given as input to the
        # shell script. Needs to be a string
        self._log.debug("Constructing SPARK command")
        self._log.debug('Opaque Slots {0}'.format(slots))

        if 'lm_info' not in slots:
            raise RuntimeError('No lm_info to launch via %s: %s'
                    % (self.name, slots))

        if not slots['lm_info']:
            raise RuntimeError('lm_info missing for %s: %s'
                               % (self.name, slots))

        if 'master_ip' not in slots['lm_info']:
            raise RuntimeError('master_ip not in lm_info for %s: %s'
                    % (self.name, slots))


        # master_ip = slots['lm_info']['master_ip']

        if task_env:
            env_string = ''
            for key,val in task_env.iteritems():
                env_string += '-shell_env ' + key + '=' + str(val) + ' '
        else:
            env_string = ''


        if task_args:
            command = " ".join(task_args)
        else:
            command = " "

        spark_configurations = " "
        # if the user hasn't specified another ui port use this one
        # if not 'spark.ui.port' in command:
        # spark_configurations += ' --conf spark.ui.port=%d '  % (random.randint(4020,4180))  

        spark_command = self.launch_command + '/' + task_exec + '  ' + spark_configurations + ' '  +  command

        self._log.debug("Spark  Command %s" % spark_command)

        return spark_command, None


# ------------------------------------------------------------------------------

