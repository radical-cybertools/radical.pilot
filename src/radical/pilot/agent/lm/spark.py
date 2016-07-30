
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import subprocess
import urllib
import sys
import socket
import random

from .base import LaunchMethod



# ==============================================================================
#
# The Launch Method Implementation for Running Spark applications
#
class Spark(LaunchMethod):

        # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, logger):

        LaunchMethod.__init__(self, cfg, logger)



    # --------------------------------------------------------------------------
    #
    @classmethod
    def lrms_config_hook(cls, name, cfg, lrms, logger):
        """
        FIXME: this config hook will inspect the LRMS nodelist and, if needed,
               will start the SPARK cluster on node[0].
        """
        import radical.utils as ru

        logger.info('Hook called by SPARK LRMS with the name %s'%lrms.name)

        # If the LRMS used is not SPARK the namenode url is going to be
        # the first node in the list and the port is the default one, else 
        # it is the one that the SPARK LRMS returns
        spark_home = None   
        if lrms.name == 'SPARKLRMS':
            #TODO: Create LRMS for existing Spark cluster
            logger.info("Found SPARK ")
            logger.info('Hook called by SPARK LRMS')
            logger.info('NameNode: %s',lrms.namenode_url)
            rm_url         = "%s:%s" % (lrms.rm_ip, lrms.rm_port)
            rm_ip          = lrms.rm_ip
            launch_command = ru.which('spark')

        else:
            # Here are the necessary commands to start the cluster.
            
            VERSION = "1.5.2"
            SPARK_DOWNLOAD_URL= "http://d3kbcqa49mib13.cloudfront.net/spark-1.5.2-bin-hadoop2.6.tgz" #prebuilt
            #Download the tar file
            opener = urllib.FancyURLopener({})
            download_destination = os.path.join(os.getcwd(),"spark-" + VERSION + ".tar.gz")
            logger.info("Download: %s to %s",SPARK_DOWNLOAD_URL, download_destination,)
            opener.retrieve(SPARK_DOWNLOAD_URL, download_destination)
            spark_tar = "spark-" + VERSION + ".tar.gz"
            if not os.path.isfile(spark_tar):
                raise RuntimeError("Spark wasn't downloaded properly. Please try again")

            os.system("tar -xzf" + spark_tar + "; rm " + spark_tar ) #untar and delete tarball 
            os.system("mv spark-1.5.2-bin-hadoop2.6 spark-1.5.2")
            spark_home = os.getcwd() + '/spark-' + VERSION

            #-------------------------------------------------------------------
            platform_os = sys.platform
            java_home = os.environ.get('JAVA_HOME')

            if platform_os == "linux" or platform_os == "linux2":
                if not java_home:
                    java = ru.which('java')
                    if java != '/usr/bin/java':
                        jpos=java.split('bin')
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

            # if no installation found install scala 2.10.4
            scala_home=ru.which('scala')
            if not scala_home:
                os.system('cd')
                os.system('wget http://www.scala-lang.org/files/archive/scala-2.10.4.tgz')
                if not os.path.isfile('scala-2.10.4.tgz'):
                    raise RuntimeError("Scala wasn't downloaded properly. Please try again")
                os.system('tar -xvf scala-2.10.4.tgz ; cd scala-2.10.4 ; export PATH=`pwd`/bin:$PATH; export SCALA_HOME=`pwd`')
                os.system('rm scala-2.10.4.tgz')
                scala_home = os.getcwd() + '/scala-2.10.4'
                os.system('cd')


            if lrms.node_list[0]!='localhost':
                hostname = subprocess.check_output('/bin/hostname').split(lrms.node_list[0])[1].split('\n')[0]

            else:
                hostname = ''

            spark_conf_slaves = open(spark_home+"/conf/slaves",'w')

            if len(lrms.node_list) == 1:
                spark_conf_slaves.write(lrms.node_list[0]+hostname)
                spark_conf_slaves.write('\n')
            else:
                for nodename in lrms.node_list[1:]:
                    spark_conf_slaves.write(nodename+hostname)
                    spark_conf_slaves.write('\n')

            spark_conf_slaves.close()

            ## put Master Ip in spark-env.sh file - 

            python_path = os.getenv('PYTHONPATH')
            python = ru.which('python')
            logger.info('Python Executable: %s' % python)
            master_ip = lrms.node_list[0]+hostname

            #Setup default env properties:
            spark_default_file = open(spark_home + "/conf/spark-defaults.conf",'w')
            spark_master_string = 'spark://%s:7077' % master_ip
            spark_default_file.write('spark.master  ' + spark_master_string + '\n')
            spark_default_file.close()
            logger.info("Let's print the config")
            logger.info('Config : {0}'.format(cfg['resource_cfg']))

            spark_env_file = open(spark_home + "/conf/spark-env.sh",'w')
            #load in the spark enviroment of master and slaves the
            #configurations of the machine
            if master_ip!='localhost':
                for config in cfg['resource_cfg']['pre_bootstrap_1']:
                    spark_env_file.write(config + '\n')

            spark_env_file.write('export SPARK_MASTER_IP=' + master_ip + "\n")
            spark_env_file.write('export SCALA_HOME='+ scala_home+ "\n")
            spark_env_file.write('export JAVA_HOME=' + java_home + "\n")
            spark_env_file.write('export SPARK_LOG_DIR='+os.getcwd()+'/spark-logs'+'\n')
            spark_env_file.write('export PYSPARK_PYTHON='+python+'\n')

            spark_env_file.close()


            #### Start spark Cluster
            spark_start = subprocess.check_output(spark_home + '/sbin/start-all.sh')
            if 'Error' in spark_start:
                raise RuntimeError("Spark Cluster failed to start: %s" % spark_start)
            
            logger.info('Start Spark Cluster')
            launch_command = spark_home +'/bin'

          
        # The LRMS instance is only available here -- everything which is later
        # needed by the scheduler or launch method is stored in an 'lm_info'
        # dict.  That lm_info dict will be attached to the scheduler's lrms_info
        # dict, and will be passed around as part of the opaque_slots structure,
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
    def lrms_shutdown_hook(cls, name, cfg, lrms, lm_info, logger):
        if 'name' not in lm_info:
            raise RuntimeError('name not in lm_info for %s' \
                    % (self.name))

        if lm_info['name'] != 'SPARKLRMS':
            logger.info('Stoping SPARK')
            stop_spark = subprocess.check_output(lm_info['spark_home'] + '/sbin/stop-all.sh') 
            if 'Error' in stop_spark:
                logger.warn("Spark didn't terminate properly")
            else:
                logger.info("Spark stopped successfully")

    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self._log.info(self._cfg['lrms_info']['lm_info'])
        self.launch_command = self._cfg['lrms_info']['lm_info']['launch_command']
        self._log.info('SPARK was called')

    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        opaque_slots = cu['opaque_slots']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_cores   = cud['cores']
        task_args    = cud.get('arguments')
        task_env     = cud.get('environment')
        work_dir     = cu['workdir']
        unit_id      = cu['_id']

        # Construct the args_string which is the arguments given as input to the
        # shell script. Needs to be a string
        self._log.debug("Constructing SPARK command")
        self._log.debug('Opaque Slots {0}'.format(opaque_slots))

        if 'lm_info' not in opaque_slots:
            raise RuntimeError('No lm_info to launch via %s: %s' \
                    % (self.name, opaque_slots))

        if not opaque_slots['lm_info']:
            raise RuntimeError('lm_info missing for %s: %s' \
                               % (self.name, opaque_slots))

        if 'master_ip' not in opaque_slots['lm_info']:
            raise RuntimeError('master_ip not in lm_info for %s: %s' \
                    % (self.name, opaque_slots))


        if 'nodename' not in opaque_slots['lm_info']:
            raise RuntimeError('nodename not in lm_info for %s: %s' \
                    % (self.name, opaque_slots))

        master_ip   = opaque_slots['lm_info']['master_ip']
        client_node = opaque_slots['lm_info']['nodename']
        #spark_home = opaque_slots['lm_info']['spark_home']

        # spark_logs = os.path.dirname(spark_home) + '/spark-logs'
        # for filename in os.listdir(spark_logs):
        #     f = subprocess.Popen(['tail','-F',filename],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        #     line = f.stdout.readline()
        #     for line in lines:
        #         if line.find('ERROR'):
        #             if not line.find('SIGTERM'):
        #                 raise RuntimeError("CU failed to execute: " + line + "\n" + "Check: "+ spark_logs + filename + "\n")


        if task_env:
            env_string = ''
            for key,val in task_env.iteritems():
                env_string+= '-shell_env '+key+'='+str(val)+' '
        else:
            env_string = ''


        if task_args:
            command = " ".join(task_args)
        else:
            command = " "

        spark_configurations = " "
        # if the user hasn't specified another ui port use this one
        if not 'spark.ui.port' in command:
            spark_configurations += ' --conf spark.ui.port=%d '  % (random.randint(4020,4180))  # can i use this range? TODO
        
        spark_command = self.launch_command + '/' + task_exec + '  ' + spark_configurations + ' '  +  command


        self._log.debug("Spark  Command %s"%spark_command)

        return spark_command, None
