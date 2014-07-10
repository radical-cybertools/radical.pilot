
.. _chapter_faq:

**************************
Frequently Asked Questions
**************************

Q: I see the error "Host key verification failed" in AGENT.STDERR.
------------------------------------------------------------------

.. code-block:: bash
    Host key verification failed.
    kill: 19932: No such process

A: Set up password-less, intra-node SSH access.
-----------------------------------------------

Even though this should already be set up by default on many HPC clusters, it
is not always the case. The following instructions will help you to set up
password-less SSH acces:

.. code-block:: bash
    cd ~/.ssh/
    ssh-keygen -t rsa

For passphrase just hit return. The result should look something like this:

.. code-block:: bash

    Generating public/private rsa key pair.
    Enter file in which to save the key (/home/e290/e290/oweidner/.ssh/id_rsa): 
    Enter passphrase (empty for no passphrase): 
    Enter same passphrase again: 
    Your identification has been saved in /home/e290/e290/oweidner/.ssh/id_rsa.
    Your public key has been saved in /home/e290/e290/oweidner/.ssh/id_rsa.pub.
    The key fingerprint is:
    73:b9:cf:45:3d:b6:a7:22:72:90:28:0a:2f:8a:86:fd oweidner@eslogin001
    The key's randomart image is:
    +--[ RSA 2048]----+
    |                 |
    |                 |
    |                 |
    |           .   . |
    |       .S.o   .o.|
    |.   . . oo . .. o|
    |.+ . .   ..   ...|
    |+.+     . oo.. ..|
    |=. .E    o .o..  |
    +-----------------+

Next, add you newly generated key to ~/.ssh/authorized_keys:

.. code-block:: bash

    cat id_rsa.pub >> ~/.ssh/authorized_keys