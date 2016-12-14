
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
# The Launch Method Implementation for Running Streaming applications
#
class Kafka(LaunchMethod):

    #--------------------------------------------------------------------------
    #
    def __init__(self, cfg, logger):

        LaunchMethod.__init__(self, cfg, logger)



    # --------------------------------------------------------------------------
    #
    @classmethod
    def lrms_config_hook(cls, name, cfg, lrms, logger):


        def lrms_apache_spark():

            if not os.environ.get('SPARK_HOME'):
                logger.info("Downloading Apache Spark..")
                try:    
                    VERSION = "2.0.2"
                    subprocess.check_call("wget http://d3kbcqa49mib13.cloudfront.net/spark-2.0.2-bin-hadoop2.7.tgz".split())
                    subprocess.check_call('tar -xzf spark-2.0.2-bin-hadoop2.7.tgz'.split())
                    subprocess.check_call(("mv spark-2.0.2-bin-hadoop2.7 spark-" + VERSION).split())
                except  Exception as e:
                    raise RuntimeError("Spark wasn't installed properly. Please try again. %s " % e )
                spark_home = os.getcwd() + '/spark-' + VERSION
            else:
                spark_home = os.environ['SPARK_HOME']


            spark_conf_slaves = open(spark_home+"/conf/slaves",'w')

            if len(lrms.node_list) == 1:
                spark_conf_slaves.write(lrms.node_list[0])#+hostname)
                spark_conf_slaves.write('\n')
            else:
                for nodename in lrms.node_list[1:]:
                    spark_conf_slaves.write(nodename)   # +hostname)
                    spark_conf_slaves.write('\n')

            spark_conf_slaves.close()

            ## put Master Ip in spark-env.sh file - 

            python_path = os.getenv('PYTHONPATH')
            python = ru.which('python')
            logger.info('Python Executable: %s' % python)
            if len(lrms.node_list) ==1:
                master_ip = lrms.node_list[0]
            else:
                try:
                    master_ip = subprocess.check_output('hostname -f'.split()).strip()
                except Exception as e:
                    raise RuntimeError("Master ip couldn't be detected. %s" % e)

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
            spark_env_file.write('export JAVA_HOME=' + java_home + "\n")
            spark_env_file.write('export SPARK_LOG_DIR='+os.getcwd()+'/spark-logs'+'\n')
            spark_env_file.write('export PYSPARK_PYTHON='+python+'\n')
            spark_env_file.close()

            #### Start spark Cluster
            try:
                subprocess.check_output(spark_home + '/sbin/start-all.sh')
            except Exception as e:
                raise RuntimeError("Spark Cluster failed to start: %s" % e)
            
            logger.info('Start Spark Cluster')
            launch_command = spark_home +'/bin'

            # The LRMS instance is only available here -- everything which is later
            # needed by the scheduler or launch method is stored in an 'lm_info'
            # dict.  That lm_info dict will be attached to the scheduler's lrms_info
            # dict, and will be passed around as part of the opaque_slots structure,
            # so it is available on all LM create_command calls.
            spark_lm_info = {'spark_home'    : spark_home,
                             'master_ip'     : master_ip,
                             'lm_detail'     : spark_master_string,
                             'name'          : lrms.name,
                             'launch_command': launch_command,
                             'nodename'      : lrms.node_list[0]
                             }

            return spark_lm_info
            
        #------------------------------------------------------------------------------

        import re
        from os import fsync
        ## this function is to update values in the configuration file os kafka
        def updating(filename,dico):

            RE = '(('+'|'.join(dico.keys())+')\s*=)[^\r\n]*?(\r?\n|\r)'
            pat = re.compile(RE)

            def jojo(mat,dic = dico ):
                return dic[mat.group(2)].join(mat.group(1,3))

            with open(filename,'rb') as f:
                content = f.read()

            with open(filename,'wb') as f:
                f.write(pat.sub(jojo,content))

         ##---------------------------------------------------

        logger.info("Downloading Apache Kafka..")
        try:
            subprocess.check_call('wget http://mirror.cc.columbia.edu/pub/software/apache/kafka/0.8.2.1/kafka_2.11-0.8.2.1.tgz'.split())
            subprocess.check_call('tar -zxf kafka_2.11-0.8.2.1.tgz'.split())
            subprocess.check_call('rm kafka_2.11-0.8.2.1.tgz'.split())
            kafka_home = os.getcwd() + '/kafka_2.11-0.8.2.1'
            logger.info("Kafka directory: %s \n " % kafka_home)
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


        path = os.path.join(kafka_home, 'tmp/zookeeper/data')
        os.system('mkdir -p ' + path)
        logger.info("Zookeeper dataDir: %s \n"  % path)


        ## fix zookeeper properties 
        zk_properties_file = open(kafka_home + '/config/zookeeper.properties','w')
        # unit for measuments properites, like heartbeats and timeouts.
        tickTime = 2000
        zk_properties_file.write('tickTime = %d \n' % tickTime)
        dataDir = kafka_home + '/tmp/zookeeper/data'   
        zk_properties_file.write('dataDir=%s \n' % dataDir )
        clientPort = 2181  
        #TODO: add only odd number of zk nodes to satisfy quorum 
        zk_properties_file.write('clientPort = %d \n' % clientPort)

        #add these lines for multinode zk setup and remove the next one
        #for i, nodename in enumerate(lrms.node_list):
        #    zk_properties_file.write('server.' + str(i)  + '=' + nodename + ':2888:3888\n') #+ '    #ex. server.1=c242.stampede:2888:3888

        zk_properties_file.write('server' + '=' + lrms.node_list[0] + '\n')
        # initial limits : tick_time/init_limit (s)  . it is the amount time that takes zk  follower to connect to a leader initially when a cluster is started
        initLimit = 5
        zk_properties_file.write('initLimit = %d \n' % initLimit)
        syncLimit = 23
        zk_properties_file.write('syncLimit = %d \n' % syncLimit)
        maxClientCnxns = 0
        #zk_properties_file.write('maxClientCnxns = %d \n' % maxClientCnxns)  ## TODO: fix this
        zk_properties_file.close()

        # prp na kanw copy paste afto to arxeio se kane node kai na alla3w to clientPort kai to dataDir
        # for i in xrange(len(lrms.node_list)):
        #     newDir =  dataDir + '/' + str(i+1)
        #     os.system('mkdir  -p' + newDir)
        #     os.system('echo ' + str(i+1)  + ' . ' + newDir + 'myid')



        nodenames_string = lrms.node_list[0]    #+  ':2181'   #TODO: this is for zk
        
        brokers_url = ''
        #setup configuration of kafka for multibroker cluster 
        for i,nodename in enumerate(lrms.node_list):
            try:
                os.system('cp ' + kafka_home +'/config/server.properties ' + kafka_home + '/config/server.properties_%d' % i)
                vars = ['broker.id','log.dirs','zookeeper.connect' ]
                new_values = [str(i),'/tmp/kafka-logs-'+str(i), nodenames_string]
                what_to_change = dict(zip(vars,new_values))
                filename = kafka_home + '/config/server.properties_' + str(i)
                updating(filename,what_to_change)
                with open(filename,'a') as f:
                    f.write('\n ## added by Radical-Pilot  ## \n')
                    f.write('host.name=%s\n' % nodename )
                    f.write('delete.topic.enable = true\n')
            except Exception as e:
                raise RuntimeError(e)


        #### Start Zookeeper Cluster Service
        zk_properties_path = os.path.join(kafka_home, 'config/zookeeper.properties')
        logger.info('Zk properties path: %s  \n' % zk_properties_path ) 


        logger.info('Starting Zookeeper service..')
        try:
            os.system(kafka_home + '/bin/zookeeper-server-start.sh ' + ' -daemon  ' + zk_properties_path)
        except Exception as e:
            raise RuntimeError("Zookeeper service failed to start: %s " % e)

        ## start kafka server:
        logger.info('Starting Kafka service..')
        try:
            for i,nodename in enumerate(lrms.node_list):
                os.system(kafka_home + '/bin/kafka-server-start.sh' + '  -daemon  ' + kafka_home + '/config/server.properties_%d' %i )
        except Exception as e:
            raise RuntimeError("Kafka service failed to start: %s" % e)

        launch_command = kafka_home + '/bin'

        zookeeper_url_string = nodenames_string
        spark_lm_info = lrms_apache_spark()

        lm_detail_dict = {'zk_url': zookeeper_url_string, 'brokers': lrms.node_list, 
                                                          'spark_master': spark_lm_info['lm_detail']}



        

          
        # The LRMS instance is only available here -- everything which is later
        # needed by the scheduler or launch method is stored in an 'lm_info'
        # dict.  That lm_info dict will be attached to the scheduler's lrms_info
        # dict, and will be passed around as part of the opaque_slots structure,
        # so it is available on all LM create_command calls.
        lm_info = {'kafka_home'    : kafka_home,
                   'lm_detail'     : lm_detail_dict,       
                   'zk_url'        : zookeeper_url_string,
                   'name'          : lrms.name,
                   'launch_command': launch_command,
                   'nodename'      : lrms.node_list[0],
                   'spark_home'    : spark_lm_info['spark_home'],
                   'master_ip'     : spark_lm_info['master_ip'],
                   'spark_launch'  : spark_lm_info['launch_command']

                   }

        return lm_info


    # --------------------------------------------------------------------------
    #
    @classmethod    ## i have to shutdown kafka too
    def lrms_shutdown_hook(cls, name, cfg, lrms, lm_info, logger):
        if 'name' not in lm_info:
            raise RuntimeError('name not in lm_info for %s' \
                    % (self.name))

        if lm_info['name'] != 'KAFKALRMS':
            logger.info('Stoping Zookeeper')
            try:
                stop_kafka = os.system(lm_info['kafka_home'] + '/bin/zookeeper-server-stop.sh') 
            except Exception as e:
                raise RuntimeError("Zookeeper failed to stop properly.")
            else:
                logger.info('Zookeeper stopped successfully')
                ## TODO: stop kafka server too
            logger.info('Stoping SPARK')
            try:
                os.system(lm_info['spark_home'] + '/sbin/stop-all.sh') 
            except Exception as e:
                raise RuntimeError("Spark failed to terminate properly.")
            else:
                logger.info("Spark stopped successfully")

    # --------------------------------------------------------------------------
    #
    #TODO: what is configure responsible for?
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

        if 'master_ip' not in opaque_slots['lm_info']:
            raise RuntimeError('master_ip not in lm_info for %s: %s' \
                    % (self.name, opaque_slots))

        if 'spark_launch' not in opaque_slots['lm_info']:
            raise RuntimeError('spark_launch not in lm_info for %s: %s' \
                    % (self.name, opaque_slots))

        if 'zk_url' not in opaque_slots['lm_info']:
            raise RuntimeError('zk_url not in lm_info for %s: %s' \
                    % (self.name, opaque_slots))
        



        master_ip   = opaque_slots['lm_info']['master_ip']
        spark_launch = opaque_slots['lm_info']['spark_launch']
        zookeeper = opaque_slots['lm_info']['zk_url']


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

        if task_exec=='spark-submit':   #TODO: fix launch commands               1.5.2
            command =  spark_launch + '/' + task_exec  + ' '  +  command  + ' '
        else:
            zk = ' --zookeeper ' + zookeeper
            command = self.launch_command  + '/'  + task_exec + ' ' + command + ' '  + zk

        print command
        self._log.debug("Command %s"%command)

        return command, None
