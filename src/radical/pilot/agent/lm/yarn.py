
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import subprocess

from .base import LaunchMethod


# ==============================================================================
#
# The Launch Method Implementation for Running YARN applications
#
class Yarn(LaunchMethod):

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
               will start the YRN cluster on node[0].
        """

        logger.info('Hook called by YARN LRMS with the name %s'%lrms.name)

        def config_core_site(node):

            core_site_file = open(os.getcwd()+'/hadoop/etc/hadoop/core-site.xml','r')
            lines = core_site_file.readlines()
            core_site_file.close()

            prop_str  = '<property>\n'
            prop_str += '  <name>fs.default.name</name>\n'
            prop_str += '    <value>hdfs://%s:54170</value>\n'%node
            prop_str += '</property>\n'

            lines.insert(-1,prop_str)

            core_site_file = open(os.getcwd()+'/hadoop/etc/hadoop/core-site.xml','w')
            for line in lines:
                core_site_file.write(line)
            core_site_file.close()

        def config_hdfs_site(nodes):

            hdfs_site_file = open(os.getcwd()+'/hadoop/etc/hadoop/hdfs-site.xml','r')
            lines = hdfs_site_file.readlines()
            hdfs_site_file.close()

            prop_str  = '<property>\n'
            prop_str += ' <name>dfs.replication</name>\n'
            prop_str += ' <value>1</value>\n'
            prop_str += '</property>\n'

            prop_str += '<property>\n'
            prop_str += '  <name>dfs.name.dir</name>\n'
            prop_str += '    <value>file:///tmp/hadoop/hadoopdata/hdfs/namenode</value>\n'
            prop_str += '</property>\n'

            prop_str += '<property>\n'
            prop_str += '  <name>dfs.data.dir</name>\n'
            prop_str += '    <value>file:///tmp/hadoop/hadoopdata/hdfs/datanode</value>\n'
            prop_str += '</property>\n'

            lines.insert(-1,prop_str)

            hdfs_site_file = open(os.getcwd()+'/hadoop/etc/hadoop/hdfs-site.xml','w')
            for line in lines:
                hdfs_site_file.write(line)
            hdfs_site_file.close()

        def config_mapred_site():

            mapred_site_file = open(os.getcwd()+'/hadoop/etc/hadoop/mapred-site.xml.template','r')
            lines = mapred_site_file.readlines()
            mapred_site_file.close()

            prop_str  = ' <property>\n'
            prop_str += '  <name>mapreduce.framework.name</name>\n'
            prop_str += '   <value>yarn</value>\n'
            prop_str += ' </property>\n'

            lines.insert(-1,prop_str)

            mapred_site_file = open(os.getcwd()+'/hadoop/etc/hadoop/mapred-site.xml','w')
            for line in lines:
                mapred_site_file.write(line)
            mapred_site_file.close()

        def config_yarn_site():

            yarn_site_file = open(os.getcwd()+'/hadoop/etc/hadoop/yarn-site.xml','r')
            lines = yarn_site_file.readlines()
            yarn_site_file.close()

            prop_str  = ' <property>\n'
            prop_str += '  <name>yarn.nodemanager.aux-services</name>\n'
            prop_str += '    <value>mapreduce_shuffle</value>\n'
            prop_str += ' </property>\n'

            lines.insert(-1,prop_str)

            yarn_site_file = open(os.getcwd()+'/hadoop/etc/hadoop/yarn-site.xml','w')
            for line in lines:
                yarn_site_file.write(line)
            yarn_site_file.close()

        # If the LRMS used is not YARN the namenode url is going to be
        # the first node in the list and the port is the default one, else
        # it is the one that the YARN LRMS returns
        hadoop_home = None
        if lrms.name == 'YARNLRMS': # FIXME: use constant
            logger.info('Hook called by YARN LRMS')
            logger.info('NameNode: {0}'.format(lrms.namenode_url))
            service_url    = lrms.namenode_url
            rm_url         = "%s:%s" % (lrms.rm_ip, lrms.rm_port)
            rm_ip          = lrms.rm_ip
            launch_command = cls._which('yarn')

        else:
            # Here are the necessary commands to start the cluster.
            if lrms.node_list[0] == 'localhost':
                #Download the tar file
                node_name = lrms.node_list[0]
                stat = os.system("wget http://apache.claz.org/hadoop/common/hadoop-2.6.0/hadoop-2.6.0.tar.gz")
                stat = os.system('tar xzf hadoop-2.6.0.tar.gz;mv hadoop-2.6.0 hadoop;rm -rf hadoop-2.6.0.tar.gz')
            else:
                node = subprocess.check_output('/bin/hostname')
                logger.info('Entered Else creation')
                node_name = node.split('\n')[0]
                stat = os.system("wget http://apache.claz.org/hadoop/common/hadoop-2.6.0/hadoop-2.6.0.tar.gz")
                stat = os.system('tar xzf hadoop-2.6.0.tar.gz;mv hadoop-2.6.0 hadoop;rm -rf hadoop-2.6.0.tar.gz')
                # TODO: Decide how the agent will get Hadoop tar ball.

                # this was formerly
                #   def set_env_vars():
                # but we are in a class method, and don't have self -- and we don't need
                # it anyway...

            hadoop_home        = os.getcwd() + '/hadoop'
            hadoop_install     = hadoop_home
            hadoop_mapred_home = hadoop_home
            hadoop_common_home = hadoop_home
            hadoop_hdfs_home   = hadoop_home
            yarn_home          = hadoop_home

            hadoop_common_lib_native_dir = hadoop_home + '/lib/native'

            #-------------------------------------------------------------------
            # Solution to find Java's home folder:
            # http://stackoverflow.com/questions/1117398/java-home-directory

            jpos = subprocess.check_output(['readlink','-f', '/usr/bin/java']).split('bin')
            if jpos[0].find('jre') != -1:
                java_home = jpos[0][:jpos[0].find('jre')]
            else:
                java_home = jpos[0]

            hadoop_env_file = open(hadoop_home+'/etc/hadoop/hadoop-env.sh','r')
            hadoop_env_file_lines = hadoop_env_file.readlines()
            hadoop_env_file.close()
            hadoop_env_file_lines[24] = 'export JAVA_HOME=%s'%java_home
            hadoop_env_file = open(hadoop_home+'/etc/hadoop/hadoop-env.sh','w')
            for line in hadoop_env_file_lines:
                hadoop_env_file.write(line)
            hadoop_env_file.close()

            # set_env_vars() ended here

            config_core_site(node_name)
            config_hdfs_site(lrms.node_list)
            config_mapred_site()
            config_yarn_site()

            logger.info('Start Formatting DFS')
            namenode_format = os.system(hadoop_home + '/bin/hdfs namenode -format -force')
            logger.info('DFS Formatted. Starting DFS.')
            hadoop_start = os.system(hadoop_home + '/sbin/start-dfs.sh')
            logger.info('Starting YARN')
            yarn_start = os.system(hadoop_home + '/sbin/start-yarn.sh')

            #-------------------------------------------------------------------
            # Creating user's HDFS home folder
            logger.debug('Running: %s/bin/hdfs dfs -mkdir /user'%hadoop_home)
            os.system('%s/bin/hdfs dfs -mkdir /user'%hadoop_home)
            uname = subprocess.check_output('whoami').split('\n')[0]
            logger.debug('Running: %s/bin/hdfs dfs -mkdir /user/%s'%(hadoop_home,uname))
            os.system('%s/bin/hdfs dfs -mkdir /user/%s'%(hadoop_home,uname))
            check = subprocess.check_output(['%s/bin/hdfs'%hadoop_home,'dfs', '-ls', '/user'])
            logger.info(check)
            # FIXME YARN: why was the scheduler configure called here?  Configure
            #             is already called during scheduler instantiation
            # self._scheduler._configure()

            service_url = node_name + ':54170'
            rm_url      = node_name
            launch_command = yarn_home + '/bin/yarn'
            rm_ip = node_name


        # The LRMS instance is only available here -- everything which is later
        # needed by the scheduler or launch method is stored in an 'lm_info'
        # dict.  That lm_info dict will be attached to the scheduler's lrms_info
        # dict, and will be passed around as part of the opaque_slots structure,
        # so it is available on all LM create_command calls.
        lm_info = {'service_url'   : service_url,
                   'rm_url'        : rm_url,
                   'hadoop_home'   : hadoop_home,
                   'rm_ip'         : rm_ip,
                   'name'          : lrms.name,
                   'launch_command': launch_command,
                   'nodename'      : lrms.node_list[0] }

        return lm_info


    # --------------------------------------------------------------------------
    #
    @classmethod
    def lrms_shutdown_hook(cls, name, cfg, lrms, lm_info, logger):
        if 'name' not in lm_info:
            raise RuntimeError('name not in lm_info for %s' % name)

        if lm_info['name'] != 'YARNLRMS': # FIXME: use constant
            logger.info('Stoping YARN')
            os.system(lm_info['hadoop_home'] + '/sbin/stop-yarn.sh')

            logger.info('Stoping DFS.')
            os.system(lm_info['hadoop_home'] + '/sbin/stop-dfs.sh')

            logger.info("Deleting HADOOP files from temp")
            os.system('rm -rf /tmp/hadoop*')
            os.system('rm -rf /tmp/Jetty*')
            os.system('rm -rf /tmp/hsperf*')


    # --------------------------------------------------------------------------
    #
    def _configure(self):

        # Single Node configuration
        # TODO : Multinode config
        self._log.info('Getting YARN app')
        os.system('wget https://dl.dropboxusercontent.com/u/28410803/Pilot-YARN-0.1-jar-with-dependencies.jar')
        self._log.info(self._cfg['lrms_info']['lm_info'])
        self.launch_command = self._cfg['lrms_info']['lm_info']['launch_command']
        self._log.info('YARN was called')


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        opaque_slots = cu['opaque_slots']
        work_dir     = cu['workdir']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_cores   = cud['cores']
        task_env     = cud.get('environment') or {}
        task_args    = cud.get('arguments')   or []
        task_argstr  = self._create_arg_string(task_args)

        # Construct the args_string which is the arguments given as input to the
        # shell script. Needs to be a string
        self._log.debug("Constructing YARN command")
        self._log.debug('Opaque Slots {0}'.format(opaque_slots))

        if 'lm_info' not in opaque_slots:
            raise RuntimeError('No lm_info to launch via %s: %s' \
                    % (self.name, opaque_slots))

        if not opaque_slots['lm_info']:
            raise RuntimeError('lm_info missing for %s: %s' \
                               % (self.name, opaque_slots))

        if 'service_url' not in opaque_slots['lm_info']:
            raise RuntimeError('service_url not in lm_info for %s: %s' \
                    % (self.name, opaque_slots))

        if 'rm_url' not in opaque_slots['lm_info']:
            raise RuntimeError('rm_url not in lm_info for %s: %s' \
                    % (self.name, opaque_slots))


        if 'nodename' not in opaque_slots['lm_info']:
            raise RuntimeError('nodename not in lm_info for %s: %s' \
                    % (self.name, opaque_slots))

        service_url = opaque_slots['lm_info']['service_url']
        rm_url      = opaque_slots['lm_info']['rm_url']
        client_node = opaque_slots['lm_info']['nodename']

        #-----------------------------------------------------------------------
        # Create YARN script
        # This funcion creates the necessary script for the execution of the
        # CU's workload in a YARN application. The function is responsible
        # to set all the necessary variables, stage in, stage out and create
        # the execution command that will run in the distributed shell that
        # the YARN application provides. There reason for staging out is
        # because after the YARN application has finished everything will be
        # deleted.

        print_str ="echo '#!/usr/bin/env bash'>>ExecScript.sh\n"
        print_str+="echo ''>>ExecScript.sh\n"
        print_str+="echo ''>>ExecScript.sh\n"
        print_str+="echo '#---------------------------------------------------------'>>ExecScript.sh\n"
        print_str+="echo '# Staging Input Files'>>ExecScript.sh\n"

        self._log.debug('Creating input staging')
        if cud['input_staging']:
            scp_input_files='"'
            for InputFile in cud['input_staging']:
                scp_input_files+='%s/%s '%(work_dir,InputFile['target'])
            scp_input_files+='"'
            print_str+="echo 'scp $YarnUser@%s:%s .'>>ExecScript.sh\n"%(client_node,scp_input_files)

        print_str+="echo ''>>ExecScript.sh\n"
        print_str+="echo ''>>ExecScript.sh\n"
        print_str+="echo '#---------------------------------------------------------'>>ExecScript.sh\n"
        print_str+="echo '# Creating Executing Command'>>ExecScript.sh\n"
        
        print_str+="echo '%s %s 1>Ystdout 2>Ystderr'>>ExecScript.sh\n"%(cud['executable'],task_argstr)

        print_str+="echo ''>>ExecScript.sh\n"
        print_str+="echo ''>>ExecScript.sh\n"
        print_str+="echo '#---------------------------------------------------------'>>ExecScript.sh\n"
        print_str+="echo '# Staging Output Files'>>ExecScript.sh\n"
        print_str+="echo 'YarnUser=$(whoami)'>>ExecScript.sh\n"
        scp_output_files='Ystderr Ystdout'

        if cud['output_staging']:
            for OutputFile in cud['output_staging']:
                scp_output_files+=' %s'%(OutputFile['source'])
        print_str+="echo 'scp -v %s $YarnUser@%s:%s'>>ExecScript.sh\n"%(scp_output_files,client_node,work_dir)

        print_str+="echo ''>>ExecScript.sh\n"
        print_str+="echo ''>>ExecScript.sh\n"
        print_str+="echo '#End of File'>>ExecScript.sh\n\n\n"

        env_string = ''
        for key,val in task_env.iteritems():
            env_string+= '-shell_env '+key+'='+str(val)+' '

        #app_name = '-appname '+ cud['_id']
        # Construct the ncores_string which is the number of cores used by the
        # container to run the script
        if task_cores:
            ncores_string = '-container_vcores '+str(task_cores)
        else:
            ncores_string = ''

        # Construct the nmem_string which is the size of memory used by the
        # container to run the script
        #if task_nummem:
        #    nmem_string = '-container_memory '+task_nummem
        #else:
        #    nmem_string = ''

        #Getting the namenode's address.
        service_url = 'yarn://%s?fs=hdfs://%s'%(rm_url, service_url)

        yarn_command = '%s -jar ../Pilot-YARN-0.1-jar-with-dependencies.jar'\
                       ' com.radical.pilot.Client -jar ../Pilot-YARN-0.1-jar-with-dependencies.jar'\
                       ' -shell_script ExecScript.sh %s %s -service_url %s\ncat Ystdout' % (self.launch_command,
                        env_string, ncores_string,service_url)

        self._log.debug("Yarn Command %s"%yarn_command)

        return print_str+yarn_command, None


# ------------------------------------------------------------------------------

