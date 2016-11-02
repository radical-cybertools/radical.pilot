
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

        # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, logger):

        LaunchMethod.__init__(self, cfg, logger)



    # --------------------------------------------------------------------------
    #
    @classmethod
    def lrms_config_hook(cls, name, cfg, lrms, logger):


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
        # unit for measuments properites, like heartbeats and timeouts.
        tickTime = 2000
        zk_properties_file.write('tickTime = %d \n' % tickTime)
        dataDir = '/tmp/zookeeper/data'  ## TODO: isws tha prp na ginei /tmp/zookeeper/data  
        zk_properties_file.write('DataDir = %s \n' % dataDir )
        clientPort = 2181  ##
        #TODO: add only odd number of zk nodes to satisfy quorum 
        zk_properties_file.write('clientPort = %d \n')
        for i, nodename in enumerate(lrms.node_list):
            zk_properties_file.write('server.'+str(i+1) + '=' + nodename + ':2888:3888 \n')    #ex. server.1=c242.stampede:2888:3888
        # initial limits : tick_time/init_limit (s)  . it is the amount time that takes zk  follower to connect to a leader initially when a cluster is started
        initLimit = 5
        zk_properties_file.write('initLimit = %d \n')
        syncLimit = 2
        zk_properties_file.write('syncLimit = %d \n')
        maxClientCnxns = 0
        zk_properties_file.write('maxClientCnxns = %d \n', maxClientCnxns)  ##
        zk_properties_file.close()

        # prp na kanw copy paste afto to arxeio se kane node kai na alla3w to clientPort kai to dataDir
        for i in xrange(len(lrms.node_list)):
            newDir =  dataDir + '/' + str(i+1)
            subprocess.check_call('mkdir ' + newDir)
            subprocess.check_call('echo ' + str(i+1) + ' > '  + newDir + 'myid')



        nodenames_string = ''
        for nodename in lrms.node_list:
            nodenames_string += nodename + ':2080,'

        #setup configuration of kafka for multibroker cluster 
        for i,nodename in enumerate(lrms.node_list):
            try:
                subprocess.check_call('cp ' + kafka_home +'/config/server.properties ' +kafka_home + '/config/server.properties_%d',i)
                vars = ['broker.id','log.dirs','zookeeper.connect' ]
                new_values = [str(i),'/tmp/kafka-logs-'+str(i), nodenames_string]
                what_to_change = dict(zip(vars,new_values))
                filename = kafka_home + 'config/server.properties_' + str(i)
                updating(filename,what_to_change)
                with open(filename,'a') as f:
                    f.write('\n ## added by Radical-Pilot  ## \n')
                    f.write('host.name=%s \n', nodename )
                    f.write('delete.topic.enable = true\n')
            except Exception as e:
                raise RuntimeError(e)


        #### Start Zookeeper Cluster Service
        logger.info('Starting Zookeeper service..')
        try:
            subprocess.check_call(kafka_home + '/bin/zookeeper-server-start.sh ' + ' ' + kafka_home + '/config/zookeeper.properties')
        except Exception as e:
            raise RuntimeError("Zookeeper service failed to start: %s " % e)

        ## start kafka server:
        logger.info('Starting Kafka service..')
        try:
            for i,nodename in enumerate(lrms.node_list):
                subprocess.check_call(kafka_home + '/bin/kafka-server-start.sh' + ' ' + kafka_home + '/config/server.properties_%d',i )
        except Exception as e:
            raise RuntimeError("Kafka service failed to start: %s" % e)

        launch_command = kafka_home + '/bin'

        zookeeper_url_string = ""

        

          
        # The LRMS instance is only available here -- everything which is later
        # needed by the scheduler or launch method is stored in an 'lm_info'
        # dict.  That lm_info dict will be attached to the scheduler's lrms_info
        # dict, and will be passed around as part of the opaque_slots structure,
        # so it is available on all LM create_command calls.
        lm_info = {'kafka_home'    : kafka_home,
                   'lm_detail'     : zookeeper_url_string,
                   'name'          : lrms.name,
                   'launch_command': launch_command,
                   'nodename'      : lrms.node_list[0]}

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
                stop_kafka = subprocess.check_output(lm_info['kafka_home'] + '/bin/zookeeper-server-stop.sh') 
            except Exception as e:
                raise RuntimeError("Zookeeper failed to stop properly.")
            else:
                logger.info('Zookeeper stopped successfully')

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

        
        kafka_command = self.launch_command  + '/' + task_exec


        self._log.debug("Kafka  Command %s"%spark_command)

        return kafka_command, None
