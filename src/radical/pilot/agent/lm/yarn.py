
__copyright__ = "Copyright 2016, http://radical.rutgers.edu"
__license__   = "MIT"


import os
import subprocess

import radical.utils as ru

from .base import LaunchMethod


# ==============================================================================
#
# The Launch Method Implementation for Running YARN applications
#
class Yarn(LaunchMethod):

    # --------------------------------------------------------------------------
    #
    def __init__(self, cfg, session):

        LaunchMethod.__init__(self, cfg, session)


    # --------------------------------------------------------------------------
    #
    @classmethod
    def lrms_config_hook(cls, name, cfg, lrms, logger, profile):
        """
        FIXME: this config hook will inspect the LRMS nodelist and, if needed,
               will start the YARN cluster on node[0].
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

        def config_yarn_site(cores,nodelist,hostname):

            yarn_site_file = open(os.getcwd()+'/hadoop/etc/hadoop/yarn-site.xml','r')
            lines = yarn_site_file.readlines()
            yarn_site_file.close()

            total_mem_str=subprocess.check_output(['grep','MemTotal','/proc/meminfo'])
            total_free_mem=int(total_mem_str.split()[1])/1048

            if nodelist.__len__() == 1:
                cores_used = cores/2
                total_mem = total_free_mem*0.75
            else:
                cores_used = cores*(len(nodelist)-1)
                total_mem = total_free_mem*(len(nodelist)-1)
                slaves = open(os.getcwd()+'/hadoop/etc/hadoop/slaves','w')
                for node in nodelist[1:]:
                    slaves.write('%s\n'%(node+hostname))
                slaves.close()
                master = open(os.getcwd()+'/hadoop/etc/hadoop/masters','w')
                master.write('%s\n'%(nodelist[0]+hostname))
                master.close()

            max_app_mem = total_mem/cores_used

            prop_str  = ' <property>\n'
            prop_str += '  <name>yarn.nodemanager.aux-services</name>\n'
            prop_str += '    <value>mapreduce_shuffle</value>\n'
            prop_str += ' </property>\n'

            prop_str += ' <property>\n'
            prop_str += '  <name>yarn.scheduler.maximum-allocation-mb</name>\n'
            prop_str += '   <value>%d</value>\n'%max_app_mem
            prop_str += ' </property>\n'

            prop_str += ' <property>\n'
            prop_str += '  <name>yarn.resourcemanager.hostname</name>\n'
            prop_str += '   <value>%s</value>\n'%(nodelist[0]+hostname)
            prop_str += ' </property>\n'

            prop_str += ' <property>\n'
            prop_str += '  <name>yarn.nodemanager.resource.cpu-vcores</name>\n'
            prop_str += '   <value>%d</value>\n'%cores_used
            prop_str += ' </property>\n'

            prop_str += ' <property>\n'
            prop_str += '  <name>yarn.nodemanager.resource.memory-mb</name>\n'
            prop_str += '   <value>%d</value>\n'%total_mem
            prop_str += ' </property>\n'

            lines.insert(-1,prop_str)

            yarn_site_file = open(os.getcwd()+'/hadoop/etc/hadoop/yarn-site.xml','w')
            for line in lines:
                yarn_site_file.write(line)
            yarn_site_file.close()

            scheduler_file=open(os.getcwd()+'/hadoop/etc/hadoop/capacity-scheduler.xml','r')
            lines=scheduler_file.readlines()
            scheduler_file.close()

            for line in lines:
                if line.startswith('    <value>org.apache.hadoop.yarn.util.resource.'):
                    new_line='    <value>org.apache.hadoop.yarn.util.resource.'+'DefaultResourceCalculator</value>\n'
                    lines[lines.index(line)]=new_line
                elif line.startswith('    <name>yarn.scheduler.capacity.maximum-am-resource-percent</name>'):
                    new_line='    <value>1</value>\n'
                    lines[lines.index(line)+1]=new_line
                        
            scheduler_file=open(os.getcwd()+'/hadoop/etc/hadoop/capacity-scheduler.xml','w')
            for line in lines:
                scheduler_file.write(line)
            
            scheduler_file.close()

        # If the LRMS used is not YARN the namenode url is going to be
        # the first node in the list and the port is the default one, else
        # it is the one that the YARN LRMS returns
        hadoop_home = None
        if lrms.name == 'YARNLRMS': # FIXME: use constant
            logger.info('Hook called by YARN LRMS')
            logger.info('NameNode: %s', lrms.namenode_url)
            service_url    = lrms.namenode_url
            rm_url         = "%s:%s" % (lrms.rm_ip, lrms.rm_port)
            rm_ip          = lrms.rm_ip
            launch_command = ru.which('yarn')

        else:
            # Here are the necessary commands to start the cluster.
            if lrms.node_list[0] == 'localhost':
                #Download the tar file
                node_name = lrms.node_list[0]
                stat = os.system("wget http://apache.claz.org/hadoop/common/hadoop-2.6.5/hadoop-2.6.5.tar.gz")
                stat = os.system('tar xzf hadoop-2.6.5.tar.gz;mv hadoop-2.6.5 hadoop;rm -rf hadoop-2.6.5.tar.gz')
            else:
                node = subprocess.check_output('/bin/hostname')
                logger.info('Entered Else creation')
                node_name = node.split('\n')[0]
                stat = os.system("wget http://apache.claz.org/hadoop/common/hadoop-2.6.5/hadoop-2.6.5.tar.gz")
                stat = os.system('tar xzf hadoop-2.6.5.tar.gz;mv hadoop-2.6.5 hadoop;rm -rf hadoop-2.6.5.tar.gz')
                # TODO: Decide how the agent will get Hadoop tar ball.


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

            java = ru.which('java')
            if java != '/usr/bin/java':
                jpos=java.split('bin')
            else:
                jpos = os.path.realpath('/usr/bin/java').split('bin')

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
            host=node_name.split(lrms.node_list[0])[1]

            config_core_site(node_name)
            config_hdfs_site(lrms.node_list)
            config_mapred_site()
            config_yarn_site(lrms.cores_per_node,lrms.node_list,host) # FIXME GPU

            logger.info('Start Formatting DFS')
            namenode_format = os.system(hadoop_home + '/bin/hdfs namenode -format -force')
            logger.info('DFS Formatted. Starting DFS.')
            logger.info('Starting YARN')
            yarn_start = subprocess.check_output([hadoop_home + '/sbin/start-all.sh'])
            if 'Error' in yarn_start:
                raise RuntimeError('Unable to start YARN cluster: %s' \
                    % (yarn_start))
            else:
                logger.info('Started YARN')

            #-------------------------------------------------------------------
            # Creating user's HDFS home folder
            logger.debug('Running: %s/bin/hdfs dfs -mkdir /user'%hadoop_home)
            os.system('%s/bin/hdfs dfs -mkdir /user'%hadoop_home)
            uname = subprocess.check_output('whoami').split('\n')[0]
            logger.debug('Running: %s/bin/hdfs dfs -mkdir /user/%s'%(hadoop_home,uname))
            os.system('%s/bin/hdfs dfs -mkdir /user/%s'%(hadoop_home,uname))
            check = subprocess.check_output(['%s/bin/hdfs'%hadoop_home,'dfs', '-ls', '/user'])
            logger.info(check)
            logger.info('Getting YARN app')
            os.system('wget https://www.dropbox.com/s/9yxbj9btibgtg40/Pilot-YARN-0.1-jar-with-dependencies.jar')

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
        # dict, and will be passed around as part of the slots structure,
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
    def lrms_shutdown_hook(cls, name, cfg, lrms, lm_info, logger, profile):
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
        # FIXME : Upload App to another server, which will be always alive
        self._log.info(self._cfg['lrms_info']['lm_info'])
        self.launch_command = self._cfg['lrms_info']['lm_info']['launch_command']
        self._log.info('YARN was called')


    # --------------------------------------------------------------------------
    #
    def construct_command(self, cu, launch_script_hop):

        slots        = cu['slots']
        work_dir     = cu['workdir']
        cud          = cu['description']
        task_exec    = cud['executable']
        task_cores   = cud['cpu_processes'] * cud['cpu_threads']
        task_env     = cud.get('environment') or {}
        task_args    = cud.get('arguments')   or []
        task_argstr  = self._create_arg_string(task_args)

        # Construct the args_string which is the arguments given as input to the
        # shell script. Needs to be a string
        self._log.debug("Constructing YARN command")
        self._log.debug('Slots : %s', slots)

        if 'lm_info' not in slots:
            raise RuntimeError('No lm_info to launch via %s: %s' \
                    % (self.name, slots))

        if not slots['lm_info']:
            raise RuntimeError('lm_info missing for %s: %s' \
                               % (self.name, slots))

        if 'service_url' not in slots['lm_info']:
            raise RuntimeError('service_url not in lm_info for %s: %s' \
                    % (self.name, slots))

        if 'rm_url' not in slots['lm_info']:
            raise RuntimeError('rm_url not in lm_info for %s: %s' \
                    % (self.name, slots))


        if 'nodename' not in slots['lm_info']:
            raise RuntimeError('nodename not in lm_info for %s: %s' \
                    % (self.name, slots))

        service_url = slots['lm_info']['service_url']
        rm_url      = slots['lm_info']['rm_url']
        client_node = slots['lm_info']['nodename']

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
            print_str+="echo 'start=""`date +%s.%3N`""'>>ExecScript.sh\n"
            print_str+="echo 'scp $YarnUser@%s:%s .'>>ExecScript.sh\n"%(client_node,scp_input_files)
            print_str+="echo 'stop=""`date +%s.%3N`""'>>ExecScript.sh\n"
            print_str+="echo 'time_spent=""$(echo ""$stop - $start"" | bc)""'>>ExecScript.sh\n"
            print_str+="echo 'echo $time_spent >>Yprof'>>ExecScript.sh\n"

        if cud['pre_exec']:
            pre_exec_string = ''
            for elem in cud['pre_exec']:
                pre_exec_string += '%s;' % elem
            pre_exec_string+=''
            print_str+="echo ''>>ExecScript.sh\n"
            print_str+="echo ''>>ExecScript.sh\n"
            print_str+="echo '#---------------------------------------------------------'>>ExecScript.sh\n"
            print_str+="echo '# Pre exec'>>ExecScript.sh\n"
            print_str+="echo '%s'>>ExecScript.sh\n"%pre_exec_string
        
        print_str+="echo ''>>ExecScript.sh\n"
        print_str+="echo ''>>ExecScript.sh\n"
        print_str+="echo '#---------------------------------------------------------'>>ExecScript.sh\n"
        print_str+="echo '# Creating Executing Command'>>ExecScript.sh\n"
        print_str+="echo 'start=""`date +%s.%3N`""'>>ExecScript.sh\n"
        print_str+="echo '%s %s 1>Ystdout 2>Ystderr'>>ExecScript.sh\n"%(cud['executable'],task_argstr)
        print_str+="echo 'stop=""`date +%s.%3N`""'>>ExecScript.sh\n"
        print_str+="echo 'time_spent=""$(echo ""$stop - $start"" | bc)""'>>ExecScript.sh\n"
        print_str+="echo 'echo $time_spent >>Yprof'>>ExecScript.sh\n"

        print_str+="echo ''>>ExecScript.sh\n"
        print_str+="echo ''>>ExecScript.sh\n"
        print_str+="echo '#---------------------------------------------------------'>>ExecScript.sh\n"
        print_str+="echo '# Staging Output Files'>>ExecScript.sh\n"
        print_str+="echo 'start=""`date +%s.%3N`""'>>ExecScript.sh\n"
        print_str+="echo 'YarnUser=$(whoami)'>>ExecScript.sh\n"
        scp_output_files='Ystderr Ystdout'

        if cud['output_staging']:
            for OutputFile in cud['output_staging']:
                scp_output_files+=' %s'%(OutputFile['source'])
        print_str+="echo 'scp -v %s $YarnUser@%s:%s'>>ExecScript.sh\n"%(scp_output_files,client_node,work_dir)
        print_str+="echo 'stop=""`date +%s.%3N`""'>>ExecScript.sh\n"
        print_str+="echo 'time_spent=""$(echo ""$stop - $start"" | bc)""'>>ExecScript.sh\n"
        print_str+="echo 'echo $time_spent >>Yprof'>>ExecScript.sh\n"
        print_str+="echo 'scp -v Yprof $YarnUser@%s:%s'>>ExecScript.sh\n"%(client_node,work_dir)

        print_str+="echo ''>>ExecScript.sh\n"
        print_str+="echo ''>>ExecScript.sh\n"
        print_str+="echo '#End of File'>>ExecScript.sh\n\n\n"

        env_string = ''
        for key,val in task_env.iteritems():
            env_string+= '-shell_env '+key+'='+str(val)+' '

        #app_name = '-appname '+ cud['uid']
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
                       ' -shell_script ExecScript.sh %s %s -service_url %s' % (self.launch_command,
                        env_string, ncores_string,service_url)

        self._log.debug("Yarn Command %s"%yarn_command)

        return print_str+yarn_command, None


# ------------------------------------------------------------------------------

