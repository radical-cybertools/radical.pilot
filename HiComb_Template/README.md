# HiComb Template

## The following is not finished, but contains the key points for using RP

### Currently tested to handle 2048 CUs

### Required Input:
The user must specify the IP of the Amazon Instance. Given that the the IP address of an instance changes when it is shut down and then restarted, the responsbility of knowing which machine to attach to is up to the user. Once the user gives the correct IP address, the script should dynamically take care of the rest.

### Local Requirements
##### To run RADICAL Pilot also requires the following on the local machine (e.g. your laptop)
##### [See Installation Details Page for more details] (http://radicalpilot.readthedocs.org/en/latest/installation.html)
* Passwordless ssh: This can be setup using RSA keys
* virtualenv: This is so that there is an evironment dedicated to running Pilot. While this is not required, it is STRONGLY advised to use a virtualenv. RADICAL Pilot relies on certain versions of certain libraries. With a virutal environment, you can guarantee that there is an environment in which RADICAL Pilot can run successfully (given the proper packages are installed of course.)

### Remote Requirements
##### To run RADICAL Pilot on an instance, the user must install the following on the remote machine (using sudo su; apt-get update):
* gcc
* g++
* python-dev

The following package is not required, but is useful for know the number of cores a machine has. NOTE: You will receive an error if you request more cores than which exists on the machine itself.
* htop [a convenient version of top], to get the number of corse on the machine




Made resource_amazon_ec2.json (config)
    This requires pip install --upgrade <directory> to update RP
        Virtuale ENV must be activated




The following is information for the temporary MongoDB instance required to use RADICAL Pilot. RADICAL Pilot requires a persistent MongoDB server so that the pilot and the server can communicate and coordinate what tasks need to be completed. Below are the relevant details. The aim is transfer control of this account to the LSU_CCT_GeneLab group.

Account Name    :   LSU_CCT_GeneLab
Username        :   lsu_cct_genelab
Email           :   ming.tai.ha@gmail.com
Password        :   computing1

Database is Standard Line Sandbox
    Database Name is hicomb

Connect to Mongo (DNS):     mongo ds053838.mongolab.com:53838/hicomb -u <dbuser> -p <dbpassword>
Connect to Mongo (IP):      mongo 54.80.131.72:53838/hicomb


Connecting Mongo using driver via standard MongoDB URI:
    mongodb://<dbuser>:<dbpassword>@ds053838.mongolab.com:53838/hicomb
