
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import subprocess
import sys
import socket
import random
import radical.utils as ru


from .base import LaunchMethod



# ==============================================================================
#
# The Launch Method Implementation for Running Spark applications
#
class Kafka(LaunchMethod):

        # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, logger):

        LaunchMethod.__init__(self, cfg, logger)



    # --------------------------------------------------------------------------
    #
    @classmethod
    def lrms_config_hook(cls, name, cfg, lrms, logger):
        

	    logger.info("Downloading Apache Kafka..")
	    try:
            subprocess.check_call('wget http://mirror.cc.columbia.edu/pub/software/apache/kafka/0.8.2.1/kafka_2.11-0.8.2.1.tgz')
            subprocess.check_call('tar -zxf kafka_2.11-0.8.2.1.tgz')
            subprocess.check_call('rm kafka_2.11-0.8.2.1.tgz')
            subprocess.check_call('cd kafka_2.11-0.8.2.1/')
            kafka_home = os.getcwd()
        except Exception as e:
            raise RuntimeError("Kafka wasn't installed properly.Please try again. %s " % e)

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

        ## fix zookeeper properties 
        zk_properties_file = open(kafka_home + '/config/zookeeper.properties','w')
        tickTime = 2000
        zk_properties_file.write('tickTime = %d \n' % tickTime)
        dataDir = zookeeper_home + '/data'  ##
        zk_properties_file.write('DataDir = %s \n' % dataDir )
        clientPort = 2181  ##
        zk_properties_file.write('clientPort = %d \n')
        if len(lrms.node_list) == 1:
            zk_properties_file.write(lrms.node_list[0] + ':2888:3888' +'\n')
        else:
            for nodename in lrms.node_list[1:]:
                zk_properties_file.write(nodename + ':2888:3888' '\n')   
        initLimit = 5
        zk_properties_file.write('initLimit = %d \n')
        syncLimit = 2
        zk_properties_file.write('syncLimit = %d \n')
        zk_properties_file.write('maxClientCnxns = 0 \n')  ##
        zk_properties_file.close()

        ## fix zookeeper server properties:
        kafka_server_properties_file = open(kafka_home + '/config/server.properties','w+')
        kafka_server_properties_file.write('\n ## added by Radical-Pilot \n')
        kafka_server_properties_file.write('delete.topic.enable = true\n')


        #### Start Zookeeper Cluster Service
        logger.info('Starting Zookeeper service..')
        try:
            subprocess.check_call(kafka_home + '/bin/zookeeper-server-start.sh ' + ' ' + kafka_home + '/config/zookeeper.properties')
        except Exception as e:
            raise RuntimeError("Zookeeper service failed to start: %s " % e)

        ## start kafka server:
        logger.info('Starting Kafka service..')
        try:
            subprocess.check_call(kafka_home + '/bin/kafka-server-start.sh' + ' ' + kafka_home + '/config/server.properties')
        except Exception as e:
            raise RuntimeError("Kafka service failed to start: %s" % e)

        launch_command = None

        

          
        # The LRMS instance is only available here -- everything which is later
        # needed by the scheduler or launch method is stored in an 'lm_info'
        # dict.  That lm_info dict will be attached to the scheduler's lrms_info
        # dict, and will be passed around as part of the opaque_slots structure,
        # so it is available on all LM create_command calls.
        lm_info = {'kafka_home'    : kafka_home,
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

        if lm_info['name'] != 'KAFKALRMS':
            logger.info('Stoping Kafka')
            try:
                stop_kafka = subprocess.check_output(lm_info['spark_home'] + '/sbin/stop-all.sh') 
            except Exception as e:
                raise RuntimeError("Kafka failed to stop properly.")
            else:
                logger.info('Kafka stopped successfully')

    # --------------------------------------------------------------------------
    #
    def _configure(self):

        self._log.info(self._cfg['lrms_info']['lm_info'])
        self.launch_command = self._cfg['lrms_info']['lm_info']['launch_command']
        self._log.info('Kafka was called')

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
        self._log.debug("Constructing Kafka command")
        self._log.debug('Opaque Slots {0}'.format(opaque_slots))

        if 'lm_info' not in opaque_slots:
            raise RuntimeError('No lm_info to launch via %s: %s' \
                    % (self.name, opaque_slots))

        if not opaque_slots['lm_info']:
            raise RuntimeError('lm_info missing for %s: %s' \
                               % (self.name, opaque_slots))



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

        
        kafka_command = self.launch_command 


        self._log.debug("Kafka  Command %s"%spark_command)

        return kafka_command, None
